import sys
import pandas as pd
import re
from pyspark.sql import SparkSession
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

catalog_name = "glue_catalog"
bucket_name = "andres-lagos-bucket"
bucket_prefix = "iceberg"
database_name = "dictionary_quality"
warehouse_path = f"s3://{bucket_name}/{bucket_prefix}/"
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()
glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

class Create_Dictionary_Tables():
    "Create the tables of dictionary"
    
    def __init__(self, catalog_name: str, database_name: str) -> None:
        """
        initialize the database parameters
        Arg:
            catalog_name: the glue data catalog name
            database_name: the glye database name of dictionary data
        """
        
        self.database_name = database_name
        self.catalog_name = catalog_name
        self.warehouse_path = f"s3://{bucket_name}/{bucket_prefix}"
    
    def create_column_table(self, column_table_name: str = 'column') -> None:
        """
        check and create the column table
        Arg:
            column_table_name: name of the column's table metadata
        return:
            None
        """
        
        try:
            spark.table(f"{self.catalog_name}.{self.database_name}.{column_table_name}").limit(1).show()
        except:
            spark.sql(f"""
                CREATE TABLE {self.catalog_name}.{self.database_name}.{column_table_name}(
                    column_id string
                    , table_id string
                    , database string
                    , table string
                    , column_name string
                    , description string
                    , data_type string
                    , nullable boolean
                    , is_partitioned boolean
                    , ordinal_position int
                    , is_deleted boolean
                    )
                PARTITIONED BY (table_id)
                LOCATION '{self.warehouse_path}/{column_table_name}'
                TBLPROPERTIES (
                  'table_type'='ICEBERG',
                  'format'='parquet'
                )
                """).show()
            
    def create_table_table(self, table_table_name: str = 'table') -> None:
        """
        check and create the table table
        Arg:
            table_table_name: name of the table's table metadata
        return:
            None
        """
        
        try:
            spark.table(f"{self.catalog_name}.{self.database_name}.{table_table_name}").limit(1).show()
        except:
            spark.sql(f"""
            CREATE TABLE {self.catalog_name}.{self.database_name}.{table_table_name}(
                table_id string
                , database string
                , table string
                , description string
                , create_statement string
                , update_crawler string
                , are_columns_quoted boolean
                , classification string
                , columns_ordered string
                , compression_type string
                , delimiter string
                , object_count int
                , record_count int
                , size_key int
                , skip_line int
                , type_data string
                , location string
                , is_view boolean
                , is_deleted boolean
                )
                PARTITIONED BY (table_id)
                LOCATION '{self.warehouse_path}/{table_table_name}'
                TBLPROPERTIES (
                'table_type'='ICEBERG',
                'format'='parquet'
                )""").show()
        
    def create_tables(self) -> None:
        "Execute all of the create table process"
        self.create_column_table()
        self.create_table_table()
        

class Land_Metastore():
    """Lands the metadata for schemas listed in the database dictionary.
    
    This function will create the temporary views column_metadata, 
    table_metadata and create_metadata. These tables describe the metadata
    of the tables contained in the database list.
    
    Where a landed value is the string 'None', it represents a null value
    (pandas doesnt support a null types).
    

    Args:
        database_list (list): list of databases
    """
    def __init__(self, database_list: list) -> None:
        "Initialize the process and execute all of the steps"
        
        tables = []
        # for each database determine table list using spark catalog
        for database in database_list:
            table_list = spark.catalog.listTables(database)
            tables += [{'tableName': i.name, 'database': i.database
                        , 'description':i.description, 'tableType':i.tableType
                        , 'isTemporary':i.isTemporary} for i in table_list]

        # we dont want to document temp tales, remove from list.
        tables = [i for i in tables if not i['isTemporary']]
        self.tables = tables

    def get_column_df(self) -> pd.DataFrame:
        "Return pandas databframe of column metadata given list of tables."
       
        all_column_metadata=[]
        for table in self.tables:
            # collect column info
            column_list = spark.catalog.listColumns(table['tableName'], table['database'])
            # can infer ordinal position by incrementing a counter from 1 for each column in list.
            ordinal_position = 1
            for col in column_list: 
                # add each columns metadata to all_column_metadata list
                column_metadata = [table['database'], table['tableName']]+[col[index] for index in range(len(col))]+[ordinal_position]
                ordinal_position+=1
                all_column_metadata.append(column_metadata)

        # set column names
        column_metadata_columns = ['database', 'table','columnName','description', 'dataType','nullable','isPartitioned','isBucket', 'ordinalPosition']
        df_columns = pd.DataFrame(all_column_metadata, columns=column_metadata_columns)
        # resolve missing values
        df_columns.fillna('None', inplace=True)
        return df_columns

    def get_table_df(self) -> pd.DataFrame:
        """Return pandas dataframe of table metadata given list of tables."""

        all_table_metadata = []
        for table in self.tables:
            if table['tableType']!='VIEW':
                table_metadata = list()
                #collect result of describe detail sql command, collect as list and add to metadata list
                table_data = spark.sql(f"DESCRIBE EXTENDED {table['database']}.{table['tableName']}").collect()
                for index in table_data:
                    if index.col_name in ['Database', 'Table', 'Table Properties'
                        , 'Location', 'Storage Properties', 'Partition Provider']:
                        if index.col_name == 'Table Properties':
                            table_metadata.extend([value.split('=')[1] for value in re.split(r",(?!,)",index[1])])
                        else:
                            table_metadata.append(index[1])
                if len(table_metadata) == 19:
                    table_metadata.insert(10, '0')
                    table_metadata.insert(11, '') 
                table_metadata.append(False)
                table_metadata.append(table['description'])
                all_table_metadata.append(table_metadata)
            else:
                table_metadata = [table['database'], table['tableName'], None, None
                  , None, None, '0', None
                  , None, None, None, '0'
                  , '0', '0', '0', None, None
                  , None, None, True, table['description']]
                all_table_metadata.append(table_metadata)
        table_metadata_columns = ['database', 'table', 'crawlerDeserializerV', 'crawlerSerializarV' 
                                , 'updateCrawler', 'areColumnsQuoted', 'avgRecordSize', 'classification' 
                                , 'columnsOrdered', 'compressionType', 'delimiter', 'last_modified_by'
                                ,  'last_modified_time', 'objectCount'
                                , 'recordCount', 'sizeKey', 'skipLine', 'typeData', 'location'
                                , 'storageProperties', 'PartitionProvider', 'is_view', 'description']
        df_tables = pd.DataFrame(all_table_metadata,  columns=table_metadata_columns)
        # drop some columns we dont need
        df_tables.drop(labels = ['crawlerDeserializerV', 'crawlerSerializarV', 'storageProperties', 'PartitionProvider'], axis=1, inplace=True)
        # resolve missing values
        df_tables.fillna(value={'avgRecordSize': 0, 'objectCount': 0, 'recordCount': 0, 
            'sizeKey': 0, 'skipLine': 0}, inplace=True)
        df_tables.fillna('None', inplace=True)
        return df_tables


    def get_create_df(self) -> pd.DataFrame:
        """Return pandas databframe of create table statements given list of tables."""
        
        all_create_metadata = []
        for table in self.tables:
            create = spark.sql(f"SHOW CREATE TABLE {table['database']}.{table['tableName']}").collect()[0][0]
            table_metadata = [table['database'], table['tableName']] + [create]
            all_create_metadata.append(table_metadata)
        create_metadata_columns = ['database', 'table','createStatement']
        df_create = pd.DataFrame(all_create_metadata, columns=create_metadata_columns)
        return df_create

    def temp_view_from_pandas(self, df, view_name):
        """Create a temporary view from a pandas dataframe"""

        spark_df = spark.createDataFrame(df)
        spark_df.createOrReplaceTempView(view_name)
        print(f"Temporary table `{view_name}` created.")
        print(f"Row count: {spark_df.count()}")
        
    def main(self) -> None:
        "Execute all of the workflow"
        # collect metadata and land metadata for schema
        metadata_df = self.get_column_df()
        self.temp_view_from_pandas(metadata_df, 'Column_Metadata')
        metadata_df = self.get_table_df()
        self.temp_view_from_pandas(metadata_df, 'Table_Metadata')
        metadata_df = self.get_create_df()
        self.temp_view_from_pandas(metadata_df, 'Create_Metadata')

class Upsert_Metadata():
    "Upsert metadata tables"
    
    def __init__(self, catalog_name: str, database_name: str) -> None:
        """
        initialize the database parameters
        Arg:
            catalog_name: the glue data catalog name
            database_name: the glue database name of dictionary data
        """
        
        self.database_name = database_name
        self.catalog_name = catalog_name
        self.warehouse_path = f"s3://{bucket_name}/{bucket_prefix}"
        
    def upsert_column_data(self, column_table_name: str = 'column') -> None:
        """
        Upsert dictionary table column with all of the available data
        Arg:
            column_table_name: the name of the dictionary column's table
        Return:
            None
        """
        
        spark.sql(f"""
            MERGE INTO {catalog_name}.{database_name}.{column_table_name}  AS glue
                USING Column_Metadata AS temp
                ON glue.database = temp.database
                AND glue.table = temp.table 
                AND glue.column_name = temp.ColumnName
                WHEN MATCHED THEN UPDATE SET
                glue.description=CASE WHEN temp.description = 'None' THEN NULL ELSE temp.description END,
                glue.ordinal_position=temp.ordinalPosition,
                glue.data_type=temp.dataType,
                glue.nullable=temp.nullable,
                glue.is_partitioned=temp.isPartitioned,
                glue.is_deleted=False
            WHEN NOT MATCHED THEN INSERT
            (column_id, table_id, database, table, column_name
            , description, ordinal_position, data_type, nullable,
            is_partitioned, is_deleted)
            VALUES
                (CONCAT_WS('.', temp.database, temp.table, temp.columnName),
                CONCAT_WS('.', temp.database, temp.Table),
                temp.database,
                temp.table,
                temp.columnName,
                CASE WHEN temp.description = 'None' THEN NULL ELSE temp.description END,
                temp.ordinalPosition,
                temp.dataType,
                temp.nullable,
                temp.isPartitioned,
                False
                )
            """).show()
        
    def update_deleted_column(self, column_table_name: str = 'column') -> None:
        """
        update delete column in the column's dictionary table
        Arg:
            column_table_name: the name of the dictionary column's table
        Return:
            None
        """
        
        spark.sql(f"""CREATE OR REPLACE TEMP VIEW deleted_values AS
            SELECT glue.*
            FROM {catalog_name}.{database_name}.{column_table_name} glue
            ANTI JOIN Column_Metadata temp
                ON glue.database = temp.database
                AND glue.column_name = temp.columnName
                AND glue.table = temp.table
            WHERE glue.database IN (SELECT database FROM Column_Metadata)
            """).show()
        spark.sql(f"""
            MERGE INTO {catalog_name}.{database_name}.{column_table_name} glue
            USING deleted_values deleted
            ON glue.database = deleted.database
                AND glue.column_name = deleted.column_name
                AND glue.table = deleted.table
            WHEN MATCHED THEN UPDATE SET
            glue.is_deleted=True
            """).show()
            
    def create_table_metadata(self) -> None:
        """
        Create table's metadata
        Arg:
            column_table_name: the name of the dictionary table
        Return:
            None
        """
        
        spark.sql("""
        CREATE OR REPLACE TEMP VIEW stage_table_metadata AS
        SELECT 
            table.database || '.' || table.table AS table_id,
            table.database AS database,
            table.table AS table,
            CASE WHEN table.description ='None' THEN null ELSE table.description END AS description,
            metadata.createStatement as create_statement,
            table.updateCrawler as update_crawler,
            CASE WHEN table.areColumnsQuoted IN ('None', 'none') THEN false ELSE cast(table.areColumnsQuoted as boolean) END AS are_columns_quoted,
            CASE WHEN table.classification IN ('None', 'none') THEN null ELSE table.classification END AS classification,
            CASE WHEN table.columnsOrdered IN ('None', 'none') THEN false ELSE cast(table.columnsOrdered AS boolean) END AS columns_ordered,
            CASE WHEN table.compressionType IN ('None', 'none') THEN null ELSE table.compressionType END AS compression_type,
            CASE WHEN table.delimiter ='None' THEN null ELSE table.delimiter END AS delimiter,
            cast(table.objectCount AS integer) AS object_count,
            cast(table.recordCount AS integer) AS record_count,
            cast(table.sizeKey AS integer) AS size_key,
            cast(table.skipLine AS integer) AS skip_line,
            table.typeData AS type_data,
            table.location,
            is_view,
            False AS is_deleted
        FROM table_metadata table
        JOIN create_metadata metadata
            ON table.database = metadata.database
            AND table.table = metadata.table
        """).show()
            
    def upsert_table_data(self, table_table_name: str = 'table') -> None:
        """
        Upsert dictionary table table with all of the available data
        Arg:
            column_table_table: the name of the dictionary table's table
        Return:
            None
        """
        
        spark.sql(f"""
        MERGE INTO {catalog_name}.{database_name}.{table_table_name} glue 
        USING stage_table_metadata  temp
            ON glue.database = temp.database
            AND glue.table = temp.table 
        WHEN MATCHED THEN UPDATE SET
            glue.create_statement = temp.create_statement,
            glue.update_crawler = temp.update_crawler,
            glue.are_columns_quoted = temp.are_columns_quoted,
            glue.classification = temp.classification,
            glue.columns_ordered = temp.columns_ordered,
            glue.compression_type = temp.compression_type,
            glue.delimiter = temp.delimiter,
            glue.object_count = temp.object_count,
            glue.record_count = temp.record_count,
            glue.size_key = temp.size_key,
            glue.type_data = temp.type_data,
            glue.location = temp.location,
            glue.is_view = temp.is_view,
            glue.is_deleted = temp.is_deleted
        WHEN NOT MATCHED THEN INSERT *
        """).show()
            
    def update_delete_tables(self, table_table_name: str = 'table') -> None:
        """
        Get delete tables
        Arg:
            table_table_table: the name of the dictionary table's table
        Return:
            None
        """
        
        spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW deleted_values AS
        SELECT table.*
        FROM {catalog_name}.{database_name}.{table_table_name} table
        ANTI JOIN table_metadata temp
            ON table.database = temp.database
            AND table.table = temp.table
        """).show()
        spark.sql(f"""
        MERGE INTO {catalog_name}.{database_name}.{table_table_name} table
        USING deleted_values temp
        ON table.database = temp.database
            AND table.table = temp.table
        WHEN MATCHED THEN UPDATE SET
            table.is_deleted = True
        """).show()
    
    def upsert_tables(self) -> None:
        "Execute the upsert process"
        self.upsert_column_data()
        self.update_deleted_column()
        self.create_table_metadata()
        self.upsert_table_data()
        self.update_delete_tables()

Create_Dictionary_Tables(catalog_name, database_name).create_tables()
Land_Metastore(['data_lake']).main()
Upsert_Metadata(catalog_name, database_name).upsert_tables()
job.commit()