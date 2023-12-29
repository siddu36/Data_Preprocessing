# no hist framework which stores daily file as partitioned data in a single table in raw zone.
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import functions as f
# from pyspark.sql.types import StringType
import boto3
import logging
import sys
import configparser
from io import StringIO
import os
from awsglue.utils import getResolvedOptions
# from pyspark.sql.types import FloatType
# from pyspark.sql.types import IntegerType
# from pyspark.sql.types import LongType
# from pyspark.sql.types import DoubleType
from pyspark.sql.types import *
from pyspark.sql.functions import lit
# from logging_service import init_log
# from logging_service import log_json
from pyspark.sql.utils import AnalysisException
import logging_service

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = 'batch_date'
PROCESS_KEY = 'landing_to_raw'


def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               FEED_CONFIG_FILE_KEY,
                               SYS_CONFIG_KEY,
                               REGION_KEY,
                               BATCH_RUN_DATE])

    batch_run_date = args[BATCH_RUN_DATE]

    guid = args[GUID_KEY]

    config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])

    BOTO3_AWS_REGION = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    database = sys_config.get(args[REGION_KEY], 'database')
    tempdb = sys_config.get(args[REGION_KEY], 'tempdb')
    print("Database is: " + database)
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    raw_zone_bucket = sys_config.get(args[REGION_KEY], 'raw_bucket')

    # def log(message, args={}):
    #     log_json(
    #         process_key='landing_to_raw',
    #         args=json.dumps(args),
    #         message=message,
    #         group_name=cloudwatch_log_group,
    #         log_stream=guid
    #     )

    pre_raw_table = config.get('db_info', 'source_table')
    print("Pre raw table is: " + pre_raw_table)
    base_table = config.get('db_info', 'target_table')
    print("base table is: " + base_table)
    raw_zone_table_path = config.get('db_info', 'rz_table_path')
    primary_key = config.get('db_info', 'primary_key')
    client = boto3.client('logs', region_name=BOTO3_AWS_REGION)
    doschemavalidation = config.get('landing_zone_info', 'schemavalidation', fallback='true')
    # print('Job Name :',args['JOB_NAME'])

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=pre_raw_table + '_' + guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-ingest-into-raw-nohist')
    log_manager.log(message="Starting the ingest-to-raw job",
                    args={"environment": args[REGION_KEY], "feed": pre_raw_table})

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    partitiondate = batch_run_date
    print(partitiondate)
    # selectqry= 'select *  from ' + database + '.' + pre_raw_table
    # spark.sql(selectqry).show()
    inc_data_to_base_sql = 'insert overwrite table ' + database + '.' + base_table + ' partition (batchdate=\'' + partitiondate + '\') select /*+ COALESCE(20) */ * from ' + tempdb + '.' + pre_raw_table
    print('inc_data_to_base_sql: ' + inc_data_to_base_sql)
    log_manager.log(message="Query to insert updated data into raw table with the new partition date",
                    args={"sqltext": inc_data_to_base_sql})
    # spark.sql(inc_data_to_base_sql)
    try:
        spark.sql(inc_data_to_base_sql)
    except AnalysisException as e:
        if 'Can not create a Path from an empty string' in str(e):
            log_manager.log(message='Caught analysis exception in write to raw table',
                            args={"feed": pre_raw_table, 'exception': str(e)})
            print('Caught analysis exception in write to raw table...' + str(e))
        else:
            log_manager.log_error(message='Caught analysis exception in write to raw table',
                                  args={"feed": pre_raw_table, 'exception': str(e)})
            print('Caught analysis exception in write to raw table...' + str(e))
            sys.exit(str(e))
    
    try:
        df_raw_cnt=spark.sql("select count(*) as cnt from "+ database + "." + base_table +" where batchdate='"+partitiondate+"'")
        raw_cnt = df_raw_cnt.collect()[0]['cnt']
        log_manager.log(message="Delta Count Raw", args={"count": raw_cnt})
    except AnalysisException as e:
        print('Caught analysis exception...' + str(e))
    job.commit()
    return


def add_partition_columns(dataframe, batch_run_date):
    dataframe = dataframe.withColumn("batchdate", lit(batch_run_date))
    return dataframe


def get_select_query(database, source_table, target_table):
    client = boto3.client('glue', BOTO3_AWS_REGION)
    response = client.get_table(
        DatabaseName=database,
        Name=target_table
    )

    columns = response['Table']['StorageDescriptor']['Columns']

    select_query = "select "

    for column in columns:
        select_query += ' cast(' + column['Name'] + ' as ' + column['Type'] + '),'

    select_query = select_query[:-1]
    select_query += ' from ' + database + "." + source_table
    print("select query part : " + select_query)

    return str(select_query)


def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config


def purge_s3_bucket(bucketname, bucket_folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)
    for key in bucket.objects.filter(Prefix=bucket_folder):
        key.delete()


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
