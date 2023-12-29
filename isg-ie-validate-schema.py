#validates the schema of the daily file from source system
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
# from pyspark.sql.types import FloatType
# from pyspark.sql.types import IntegerType
# from pyspark.sql.types import LongType
# from pyspark.sql.types import DoubleType
from pyspark.sql.types import *
from pyspark.sql.functions import lit
import logging_service
# from logging_service import init_log
# from logging_service import log_json
from pyspark.sql.utils import AnalysisException
from data_preprocess_util import validate_with_schema

# logging.basicConfig(level=logging.INFO)
# logging.basicConfig(format='%(message)s')

DEFAULT_NUM_PARTITIONS = '20'
CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = 'batch_date'
PROCESS_KEY = 'landing_to_raw'
NUM_PARTS = "num_partitions"

def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config

def main():
    numpartitions = ''
    global BOTO3_AWS_REGION

    if ("--num_partitions" in sys.argv):
        args = getResolvedOptions(sys.argv,
                                  ['JOB_NAME', GUID_KEY,
                                   CONFIG_BUCKET_NAME_KEY,
                                   FEED_CONFIG_FILE_KEY,
                                   SYS_CONFIG_KEY,
                                   REGION_KEY,
                                   BATCH_RUN_DATE,
                                   NUM_PARTS])

        numpartitions = args[NUM_PARTS]

    else:
        args = getResolvedOptions(sys.argv,
                                  ['JOB_NAME', GUID_KEY,
                                   CONFIG_BUCKET_NAME_KEY,
                                   FEED_CONFIG_FILE_KEY,
                                   SYS_CONFIG_KEY,
                                   REGION_KEY,
                                   BATCH_RUN_DATE])

    if (not numpartitions or numpartitions.isspace()):
        numpartitions = DEFAULT_NUM_PARTITIONS

    batch_run_date = args[BATCH_RUN_DATE]

    guid = args[GUID_KEY]

    config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])

    BOTO3_AWS_REGION = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    database = sys_config.get(args[REGION_KEY], 'database')
    tempdb =  sys_config.get(args[REGION_KEY], 'tempdb')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    raw_zone_bucket = sys_config.get(args[REGION_KEY], 'raw_bucket')

    client_logs = boto3.client('logs', region_name=BOTO3_AWS_REGION)
    pre_raw_table = config.get('db_info', 'source_table')
    base_table = config.get('db_info', 'target_table')
    history_table = config.get('db_info', 'history_table')
    raw_zone_table_path = config.get('db_info', 'rz_table_path')
    hist_table_path = config.get('db_info', 'hist_table_path')
    primary_key = config.get('db_info', 'primary_key')
    incrementalloadflag = config.get('landing_zone_info', 'incrementalload')

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=pre_raw_table+'_'+guid,
                                             process_key=PROCESS_KEY, client=client_logs,job=args[REGION_KEY]+'-isg-ie-ingest-into-raw')
    log_manager.log(message="Starting the schema validation job",
                    args={"environment": args[REGION_KEY], "feed": pre_raw_table, "Numpartitions": numpartitions})


    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    # logger = glueContext.get_logger()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


    client = boto3.client('glue', region_name=BOTO3_AWS_REGION)
    response = client.get_table(
        DatabaseName=database,
        Name=base_table
    )

    columns = response['Table']['StorageDescriptor']['Columns']

    base_table_df_sql = "select * from " + tempdb + "." + pre_raw_table
    print (base_table_df_sql) 
    try:
        base_table_df = spark.sql(base_table_df_sql)
        schemavalidation_res=validate_with_schema(columns, database, pre_raw_table, base_table_df , spark, guid, batch_run_date, primary_key, log_manager)
        #if schemavalidation_res == 'false':
        if not schemavalidation_res:
             log_manager.log(message="Schema validation failed", args={"feed": pre_raw_table})
             sys.exit("Schema validation failed")
        else:
             log_manager.log(message="After schema validation",args={"environment":args[REGION_KEY],"feed":pre_raw_table,"schema_result":schemavalidation_res})
             log_manager.log(message="Schema validation successful", args={"feed": pre_raw_table})
    except Exception as e:
        print('Caught spark analysis exception...' + str(e))
        log_manager.log_error(message='Caught analysis exception', args={'exception': str(e)})
    job.commit()


if __name__ == '__main__':
    main()
