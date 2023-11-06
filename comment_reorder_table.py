import boto3
import sys
import pandas as pd
import re
from time import sleep
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

class Update_Metadata:
    """
    Update the metadata tables and add some descriptions
    """
    
    def __init__(self, catalog: str, dictionary_database: str, database: str, output_file: str) -> None:
        """
        initialize the application
        Arg:
            catalog: the glue catalog name
            dictionary_database: name of the quality database
            database: the database that are going to the analyze
            path_athena: the path when all of the athena output is going to be "S3 path"
        return: None
        """
        self.catalog = catalog
        self.dictionary_database = dictionary_database
        self.database = database
        self.athena_client = boto3.client(service_name='athena')
        self.output_file= output_file
        
    def reorder_columns_alphabetically(self, dictionary: str = 'column') -> None:
        """
        reorder column table
        Arg:
            dictionary: the name of the column
        return: None
        """
        spark.sql(f"""CREATE OR REPLACE TEMP VIEW reorder_columns AS
            SELECT table_id,
            column_name, 
            ROW_NUMBER() OVER (PARTITION BY table_id ORDER BY column_name) AS new_ordinal
            FROM {self.catalog}.{self.dictionary_database}.{dictionary} 
            WHERE LOWER(database) = '{self.database.lower()}'
            """)
        spark.sql(f"""MERGE INTO {self.catalog}.{self.dictionary_database}.{dictionary} old
            USING reorder_columns AS new
            ON new.column_name = old.column_name
            AND new.table_id = old.table_id
            WHEN MATCHED THEN UPDATE SET
            old.ordinal_position = new.new_ordinal""")
        spark.sql("DROP VIEW reorder_columns")
        
    def update_database(self) -> None:
        "collect list of tables in database that are not temporary"
        tables = [i['tableName'] for i in spark.sql(f'SHOW TABLES FROM {self.database}').collect() if not i['isTemporary']]
        print(f'Updating tables: {tables} \n')
        for table in tables:
            self.update_table(table=table)
            
    def execute_query(self, query: str, output_file: str = None) -> None:
        """
        Execute the query in Athena
        Arg:
            query: query to execute
            output_file: path of the S3 output
        """
        if output_file is None: output_file = self.output_file
        query_id = self.athena_client.start_query_execution(QueryString=query.replace('glue_catalog.','')
            , WorkGroup='primary',ResultConfiguration={'OutputLocation': output_file})['QueryExecutionId']
        while True:
            status = self.athena_client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
            if status != 'RUNNING': break 
            sleep(5)
            
    def update_table(self, table: str, dictionary : str = 'column') -> None:
        """
        Use the data dictionary to determine the ordinal position and comment associated
        with each column in a given table. Generate an alter table statement that applies
        these changes to the table.
        ########################################
        """
      # check if table is view, skip if so.
        if self.is_view(table):
            print(f'Cannot update {self.database}.{table} because its a view. \n')
            return

        df_column = spark.sql(f"""SELECT * FROM {self.catalog}.{self.dictionary_database}.{dictionary} 
            WHERE is_deleted = False
            AND LOWER(database) = '{self.database.lower()}'
            AND LOWER(table) = '{table.lower()}'
            ORDER BY ordinal_position, column_name""").toPandas()

        alter = []
        for cn, dt,d in zip(df_column['column_name'], df_column['data_type'], df_column['description']):
            if d:
            # if description include comment
                alter+= [f"{cn} {dt} COMMENT '{d}'"]
            else:
            # else dont include comment
                alter+= [f"`{cn}` {dt}"]
        alter = ', '.join(alter)

        query = f"ALTER TABLE {self.catalog}.{self.database}.{table} REPLACE COLUMNS ({alter})"

        print(query)
        self.execute_query(query)


    def is_view(self, table, dictionary = 'table'):
        """Check if a table exists, if so return True, if view return False."""

        df = spark.sql(f"""SELECT * 
            FROM {self.catalog}.{self.dictionary_database}.{dictionary}
            WHERE LOWER(table) = '{table.lower()}'
            AND LOWER (database) = '{self.database.lower()}'
            AND is_deleted=False""").toPandas()
        if df.shape[0]==0:
            raise Exception(f'Table {table} does not exist in dictionary')

        return df['is_view'][0]
    
cl_reorder_data = Update_Metadata(catalog='glue_catalog'
    , dictionary_database='dictionary_quality', database='data_lake'
    , output_file='s3://andres-lagos-bucket/query-athena/')
cl_reorder_data.update_database()
cl_reorder_data.reorder_columns_alphabetically()
job.commit()