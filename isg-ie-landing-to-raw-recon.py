# This job captures the ingestion details in cloud watch logs for post batch audit
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
import boto3
import logging
import sys
import configparser
from io import StringIO
import os
import json
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
import logging_service
import time

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
FLAG_KEY = "do_column_check"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = "batch_date"
PROCESS_KEY = 'landing_to_raw_recon'
POST_BATCH_AUDIT = 'post_batch_audit'  # recon or post_batch_audit


def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               FEED_CONFIG_FILE_KEY,
                               SYS_CONFIG_KEY,
                               BATCH_RUN_DATE,
                               REGION_KEY,
                               FLAG_KEY, POST_BATCH_AUDIT])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    feedname = args[FEED_CONFIG_FILE_KEY].split('/')[-1].split('.')[0]

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    guid = args[GUID_KEY]

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    if (args[POST_BATCH_AUDIT] == 'true'):
        post_batch_audit(args, spark, sys_config, config)
    elif (args[POST_BATCH_AUDIT] == 'false'):
        recon(args, spark, sys_config, config)

    job.commit()


# This function was moved from ingest-into-raw
# logs count of records after completion of ingestion job
def post_batch_audit(args, spark, sys_config, config):
    BOTO3_AWS_REGION = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    log_client = boto3.client('logs', region_name=BOTO3_AWS_REGION)
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    pre_raw_table = config.get('db_info', 'source_table')
    guid = args[GUID_KEY]
    batch_run_date = args[BATCH_RUN_DATE]
    database = sys_config.get(args[REGION_KEY], 'database')
    base_table = config.get('db_info', 'target_table')

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=pre_raw_table + '_' + guid,
                                             process_key='landing_to_raw', client=log_client,
                                             job=args[REGION_KEY] + '-isg-ie-landing-to-raw-recon')

    df_raw_cnt = spark.sql("select count(*) as cnt from " + database + "." + base_table + " where batchdate =" + "'" + batch_run_date + "'")

    row_list = df_raw_cnt.collect()

    for row in row_list:
        log_manager.log(message="Delta Count Raw", args={"count": row["cnt"]})
    return


# This is monthly/quarterly reconciliation with data obtained from source.
def recon(args, spark, sys_config, config):
    guid = args[GUID_KEY]
    batch_run_date = args[BATCH_RUN_DATE]
    do_column_check = int(args[FLAG_KEY])
    print(batch_run_date)

    BOTO3_AWS_REGION = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    database = sys_config.get(args[REGION_KEY], 'database')

    landing_zone_bucket = sys_config.get(args[REGION_KEY], 'lz_bucket')

    file_type = config.get('recon_details', 'file_extension')

    delimiter = config.get('recon_details', 'delimiter')

    input_table = config.get('recon_details', 'input_table', fallback='None')
    reconciliation_table = config.get('recon_details', 'recon_table')
    input_prefix = config.get('recon_details', 'input_prefix')
    output_prefix = config.get('recon_details', 'output_prefix')
    mapping_details = config.get('recon_details', 'mapping_details', fallback='None')
    input_path = 's3://' + landing_zone_bucket + '/' + input_prefix
    difference_path = 's3://' + landing_zone_bucket + '/' + output_prefix + '/difference/batchdate=' + batch_run_date + '/'
    info_path = 's3://' + landing_zone_bucket + '/' + output_prefix + '/additional_info/batchdate=' + batch_run_date + '/'
    primary_key = config.get('recon_details', 'primary_key')
    drop_columns = config.get('recon_details', 'drop_columns', fallback='')
    feedname = args[FEED_CONFIG_FILE_KEY].split('/')[-1].split('.')[0]
    pk_list = [key.strip().lower() for key in primary_key.split(',')]

    log_client = boto3.client('logs', region_name=BOTO3_AWS_REGION)
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=feedname + '_' + guid,
                                             process_key=PROCESS_KEY, client=log_client,
                                             job=args[REGION_KEY] + '-isg-ie-recon')

    log_manager.log(message="Starting the reconcillation job",
                    args={"environment": args[REGION_KEY], "table": reconciliation_table, "input_file_type": file_type,
                          "input_delimiter": delimiter, "primary keys": pk_list})
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_raw = spark.sql(
        "select * from " + database + "." + reconciliation_table + " where batchdate = '" + batch_run_date + "'")
    df_raw = df_raw.drop('batchdate')

    df_raw_count = spark.sql(
        "select count(*) as count,batchdate from " + database + "." + reconciliation_table + " where batchdate = '" + batch_run_date + "' group by batchdate")
    raw_count = df_raw_count.select('count').collect()[0].asDict()
    log_manager.log(message="Number of records in raw table : " + str(raw_count['count']))

    if mapping_details != 'None':
        mapping_details = json.loads(mapping_details)
        for key, value in mapping_details.items():
            df_raw = df_raw.withColumnRenamed(key, value)

    schema = df_raw.schema

    if input_table != 'None':
        df_recon = spark.sql('select * from ' + database + '.' + input_table)
    elif file_type == 'json':
        df_recon = spark.read.schema(schema).json(path=input_path)
    elif file_type == 'csv':
        df_recon = spark.read.schema(schema).csv(path=input_path, sep=delimiter)
        df_recon = df_recon.fillna('')
    else:
        log_manager.log(message="Invalid file type", args={"file type": file_type})

    input_file_count = df_recon.count()
    log_manager.log(message="Number of records in the input file : " + str(input_file_count))

    # Removing columns if required. If drop_columns is empty, then it will not drop any columns and will execute successfully
    drop_columns_list = [col.strip() for col in drop_columns.split(',')]
    df_raw = df_raw.drop(*drop_columns_list)
    df_recon = df_recon.drop(*drop_columns_list)
    log_manager.log(message="Dropped columns from the dataframe", args={"dropped columns": drop_columns_list})

    # Finding the difference
    df_diff = df_recon.subtract(df_raw)
    # log_manager.log(message="Difference is completed " + str(input_file_count))

    if df_diff.rdd.isEmpty():
        log_manager.log(message="Reconcillation completed. No differences found")
    else:
        try:
            # Write the difference file
            df_diff.write.json(difference_path)
            diff_count = df_diff.count()
            log_manager.log(message="Difference found between reconcillation file and raw table",
                            args={"raw table": reconciliation_table, "count_mismatch": str(diff_count),
                                  "Output Path": difference_path})
            if do_column_check == 1:
                # to get the mismatch column from the difference
                df_raw_rename = df_raw.select(
                    *(col(c).alias('df_raw_' + c) if c not in pk_list else col(c) for c in df_raw.columns))
                df_diff_rename = df_diff.select(
                    *(col(c).alias('df_diff_' + c) if c not in pk_list else col(c) for c in df_raw.columns))
                df_raw_subset = df_raw_rename.join(df_diff_rename, on=pk_list, how='inner')
                conditions_ = [when(col('df_raw_' + c) != col('df_diff_' + c), c).otherwise("") for c in df_diff.columns
                               if c not in pk_list]
                select_expr = [col(pk) for pk in pk_list]
                select_expr.append(array_remove(array(*conditions_), "").alias("column_name"))
                df_mismatch = df_raw_subset.select(*select_expr)
                df_mismatch = df_mismatch.withColumn('column_name', df_mismatch['column_name'].cast("string"))
                # log_manager.log(message="Mismatch is completed")

                # to get the new records in the input file
                df_new = df_diff.join(df_mismatch, on=pk_list, how='left_anti')
                df_new_records = df_new.select(pk_list)
                df_new_records = df_new_records.withColumn("column_name", lit("new record"))
                # log_manager.log(message="New record is completed")
                df_concat = df_mismatch.union(df_new_records)
                # log_manager.log(message="Concatenated is completed")
                df_concat.write.csv(info_path, header=True)
                log_manager.log(
                    message="Information on the mismatch records is written into a file",
                    args={"raw table": reconciliation_table, "Output Path": info_path})

        except AnalysisException as e:
            log_manager.log_error(message='Caught analysis exception', args={'exception': str(e)})
            sys.exit(str(e))

    # Purge reconciliation files from the reconciliation input folder
    purge_s3_bucket(landing_zone_bucket, input_prefix)
    log_manager.log(message="Purged reconciliation files")

    return


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

