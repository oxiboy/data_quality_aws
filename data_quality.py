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
from pyspark.sql import SparkSession, utils

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

#create columns in quality tables
try:
    spark.sql("""ALTER TABLE glue_catalog.dictionary_quality.column ADD COLUMNS null_test BOOLEAN, length_test INT""")
    spark.sql("""ALTER TABLE glue_catalog.dictionary_quality.table ADD COLUMNS Key STRING AFTER description""")
except utils.AnalysisException:
    print("Column already exits")
#define keys
spark.sql("""UPDATE glue_catalog.dictionary_quality.table
    SET key = 'event_id'
    WHERE table IN ('zealand', 'australia', 'usa', 'kingdom')""")

#create length test
spark.sql("""
    UPDATE glue_catalog.dictionary_quality.column
    SET length_test = 3
    WHERE column_name = 'sensor-type'
    """).collect()

#create null test
spark.sql("""
    UPDATE glue_catalog.dictionary_quality.column
    SET null_test = True
    WHERE column_name IN ('visible','event_id')
    """).collect()
#create block test
try:
    spark.sql("""CREATE TABLE glue_catalog.dictionary_quality.is_block
        (database STRING,
        table STRING,
        column_name STRING,
        test_name STRING,
        is_block BOOLEAN,
        is_deleted BOOLEAN)
        PARTITIONED BY (`database`)
        LOCATION 's3://andres-lagos-bucket/iceberg/is_block'
        TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet')""").collect()
except utils.AnalysisException:
    print("Column already exits")

class Data_Quality:
    "Check the data Quality process in this case null values and length"
    def __init__(self, database_data: str, dictionary_quality: str) -> None:
        """
        Initialize the table to execute the data quality
        Arg:
            database_data: data lake's name
            dictionary_quality: data quality's name
        Return: None
        """
        self.database_data = database_data
        self.dictionary_quality = dictionary_quality
    
    def length_test(self):
        """
        check the tables to make the length test
        Arg:
            None
        Return:
            None
        """
        sdf_length_test = spark.sql("""SELECT database, table, column_name, length_test, 
            CONCAT(column_id,'.','is_not_length_', length_test) AS test_name
            FROM glue_catalog.dictionary_quality.column 
            WHERE length_test IS NOT null AND is_deleted=false""")
        sdf_length_test.createOrReplaceTempView("length_test")
        spark.sql("SELECT * FROM length_test").show()
        
    def null_test(self):
        """
        check the tables to make the null test
        Arg:
            None
        Return:
            None
        """
        sdf_null_test = spark.sql("""SELECT database, table, column_name
            , CONCAT_WS('.',column_id,'is_null') AS test_name
            FROM glue_catalog.dictionary_quality.column
            WHERE null_test=true AND is_deleted=false""")
        sdf_null_test.createOrReplaceTempView("null_test")
        spark.sql("SELECT * FROM null_test").show()
        
    def data_quality_columns(self):
        """
        Create data quality column 
        !!This is not possible to implement using this use case because the crawler 
        created hive tables with no transactional action!!
        """
        #this is possible to implement in spark.sql future solutions "maybe"
        athena_client = boto3.client(service_name='athena')
        output_file = 's3://andres-lagos-bucket/query-athena/'
        for table in self.tables:
            query = f"ALTER TABLE data_lake.{table} ADD COLUMNS (`data_quality_tag` ARRAY<STRING>)"
            query_id = athena_client.start_query_execution(QueryString=query.replace('glue_catalog.','')
                , WorkGroup='primary',ResultConfiguration={'OutputLocation': output_file})['QueryExecutionId']
            while True:
                status = athena_client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
                if status != 'RUNNING': break 
                sleep(5)

    def get_null_tests(self, database: str, table: str, config: str = 'null_test') -> None:
        """
        Check null values
        Arg:
            database: database's name
            table: table's name
            config: temporal table name with null_test
        return: None
        """

        df = self.filtered_config_to_pandas(database, table, config)
        test_query=[]
        for col, test_name in zip(df['column_name'], df['test_name']):
            test_query+= ["CASE WHEN `{0}` IS NULL THEN '{1}' ELSE 'PASS' END".format(col, test_name)]
        return test_query

    def get_length_tests(self, database: str, table: str, config: str = 'length_test'):
        """
        make the length test
        Arg:
            database: database's name
            table: table's name
            config: temporal table name with length_test
        return: None
        """

        df = self.filtered_config_to_pandas(database, table, config)
        test_query=[]
        for col, test_name, length in zip(df['column_name'], df['test_name'], df['length_test']):
            test_query+= [f"CASE WHEN LENGTH(`{col}`) != {length} THEN '{test_name}' ELSE 'PASS' END"]
        return test_query    

    def filtered_config_to_pandas(self, database: str, table: str, config: str) -> None:
        """
        Filter quality test
        Arg:
            database: database's name
            table: table's name
            config: configure table name
        return: None
        """
        pdf_filter = spark.sql(f"""SELECT * FROM {config} 
                        WHERE LOWER(database) = LOWER('{database}')
                        AND LOWER(table) = LOWER('{table}')""").toPandas()
        return pdf_filter

    def get_key(self, database: str, table: str
        , dictionary: str = 'glue_catalog.dictionary_quality.table') -> None:
        """
        check unit keys
        Arg:
            database: database's name
            table: table's name
        Return: unique key(s) for database.table
        """
        pdf_keys = self.filtered_config_to_pandas(database, table, dictionary)
        try:
            key = pdf_keys['Key'][0]
        except IndexError as e:
            print("Key not in dictionary")
            raise e
        return key
    
    def create_quality_tables(self, tables: list) -> None:
        """
        Create the quality tables if it is not possible no change the data table
        Arg:
            tables: the name of the tables
        return:
            None
        """
        for table in tables:
            try:
                spark.sql(f"""
                CREATE TABLE glue_catalog.dictionary_quality.quality_{table}(
                    `event_id` long,
                    `data_quality_tag` ARRAY<STRING>
                    )
                    PARTITIONED BY (bucket(10, `event_id`))
                    LOCATION 's3://andres-lagos-bucket/iceberg/quality_{table}'
                    TBLPROPERTIES (
                    'table_type'='ICEBERG',
                    'format'='parquet'
                    )""").show()
                spark.sql(f"""MERGE INTO glue_catalog.dictionary_quality.quality_{table} quality
                USING data_lake.{table} as fact
                ON fact.`event_id` = quality.`event_id`
                WHEN NOT MATCHED THEN INSERT (event_id, data_quality_tag)
                values (fact.`event_id`, Null)
                """).show()
            except utils.AnalysisException:
                print(f"Table already quality_{table} already exits")
    

    def execute_tests(self, table: str
        , table_dict: str = 'glue_catalog.dictionary_quality.table') -> None:
        """
        Execute the data quality test

        Arg:
          database: database's name
          table: table's name
          column_dict: name of the column dictionary 
          table_dict: name of the table dictionary
        Return: None
        """
        #Identify the key of the database.table using the table dictionary
        key = self.get_key(self.database_data, table, table_dict)
        key_list = key.split(';')
        # for each test create a case statement that will perform the test
        # null test: CASE WHEN column IS NULL THEN 'FAIL' ELSE 'PASS' END
        # length test: CASE WHEN LEN(column)!=length THEN 'FAIL' ELD 'PASS' END
        # where FAIL would be replaced with the unique test name in the test config table
        null_tests = self.get_null_tests(self.database_data, table)
        length_tests = self.get_length_tests(self.database_data, table)
        all_tests = null_tests+length_tests
        if all_tests == []:
            all_test_query = "'PASS'"
        else:
            all_test_query = ', '.join(all_tests)   
        key_query = ', '.join(key_list)
        land = f"""
            SELECT 
            `{key_query}`, 
            ARRAY_EXCEPT(ARRAY({all_test_query}), ARRAY('PASS')) AS results,
            COUNT(`{key_query}`) OVER (PARTITION BY `{key_query}`) AS keyCount
            FROM {self.database_data}.{table}
            """
        sdf_land = spark.sql(land)
        sdf_land.createOrReplaceTempView("land_results")
        stage = f"""
            SELECT
            DISTINCT
            `{key_query}`,
            CASE WHEN keyCount=1 THEN results ELSE ARRAY('key_not_unique') END as results
            FROM land_results
            """
        sdf_stage = spark.sql(stage)
        sdf_stage.createOrReplaceTempView("stage_results")

        merge_on = ' AND '.join([f"TGT.`{k}`=SRC.`{k}`" for k in key_list])
        merge = f"""MERGE INTO glue_catalog.{self.dictionary_quality}.quality_{table} AS TGT USING 
            stage_results AS SRC
            ON {merge_on}
            WHEN MATCHED THEN UPDATE SET
            TGT.data_quality_tag=SRC.results
            """
        null_keys= f"""UPDATE glue_catalog.{self.dictionary_quality}.quality_{table} SET
            data_quality_tag = ARRAY('key_not_unique')
            WHERE CONCAT(`{key_query}`) IS NULL"""
        for query in [merge, null_keys]:
            print(query)
            spark.sql(query)
        
    def main(self, tables: str):
        "Execute the whole process"
        self.length_test()
        self.null_test()
        self.create_quality_tables(tables=tables)
        for table in tables:
            self.execute_tests(table=table)

class Blocked_Values:
    "Generate block values"

    def __init__(self) -> None:
        pass

    def generate_block_values(self) -> None:
        "Generate table blocks rows"
        sdf_triage_stage = spark.sql("""SELECT database, table, column_name, test_name
            FROM null_test
        UNION
            SELECT database, table, column_name, test_name
            FROM length_test
        UNION
            SELECT NULL AS databaseName, 
            NULL AS tableName, 
            NULL AS columnName, 
            'key_not_unique' AS test_name""")
        sdf_triage_stage.createOrReplaceTempView("triage_stage")
    
    def insert_block_values(self) -> None:
        "Insert values in the block table"
        spark.sql("""MERGE INTO glue_catalog.dictionary_quality.is_block AS TGT
            USING triage_stage AS SRC
                ON TGT.test_name=SRC.test_name
            WHEN MATCHED THEN UPDATE SET
                TGT.is_deleted=False --reactivate deleted tests
            WHEN NOT MATCHED THEN
            INSERT
                (database,
                table,
                column_name,
                test_Name,
                is_deleted,
                is_block)
                VALUES
                (SRC.database,
                SRC.table,
                SRC.column_name,
                SRC.test_name,
                False,
                False)""").collect()
        sdf_deleted_values = spark.sql("""
            SELECT t1.*
            FROM glue_catalog.dictionary_quality.is_block t1
            ANTI JOIN triage_stage t2
                ON t1.test_name=t2.test_name""")
        sdf_deleted_values.createOrReplaceTempView("deleted_values")
        # flag deleted tests with isDeleted flag
        spark.sql("""
            MERGE INTO glue_catalog.dictionary_quality.is_block AS TGT
            USING deleted_values AS SRC
                ON  TGT.test_name=SRC.test_name
            WHEN MATCHED THEN UPDATE SET
                is_deleted=True
            """).collect()
        
    def main(self):
        "execute the block main process"
        self.generate_block_values()
        self.insert_block_values()
        
Data_Quality(database_data = 'data_lake', dictionary_quality = 'dictionary_quality') \
    .main(tables=['zealand', 'australia', 'usa', 'kingdom'])
Blocked_Values().main()
job.commit()