#This job cleans up the lastest data from source system in landing and pre_raw bucket
import sys
import boto3
import configparser
from io import StringIO
import os
from datetime import datetime, date, time
import logging_service
import uuid
import json
from awsglue.utils import getResolvedOptions

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
BATCH_DATE_KEY = "batch_date"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
PROCESS_KEY = "landing_to_raw"

args = getResolvedOptions(sys.argv, [CONFIG_BUCKET_NAME_KEY, FEED_CONFIG_FILE_KEY, SYS_CONFIG_KEY, REGION_KEY, GUID_KEY,
                                     BATCH_DATE_KEY])


def main():
    global BOTO3_AWS_REGION

    print('Config file prefix : ', args[FEED_CONFIG_FILE_KEY])
    guid = args[GUID_KEY]
    config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])

    # Read config file
    lz_bucket = sys_config.get(args[REGION_KEY], 'lz_bucket')
    pre_raw_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    #pyspark_job = sys_config.get(args[REGION_KEY], 'pyspark_job')
    BOTO3_AWS_REGION = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=BOTO3_AWS_REGION)

    prerawtbname = config.get('db_info', 'source_table')
    lz_feed_file_dir = config.get('landing_zone_info', 'lz_file_prefix')
    pre_raw_dir = config.get('landing_zone_info', 'pre_raw_file_prefix')
    
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=prerawtbname+'_'+guid,
                                             process_key=PROCESS_KEY,client=client,job=args[REGION_KEY]+'-isg-ie-s3-cleanup')
    log_manager.log(message="Starting the S3 cleanup job", args={"environment": args[REGION_KEY]})

    # purge landing zone location
    print ("bucket name: "+lz_bucket+" , folder name: " +  lz_feed_file_dir)
    purge_s3_bucket(lz_bucket, lz_feed_file_dir)

    # purge pre-raw zone location
    print ("bucket name: "+pre_raw_bucket+" , folder name: " +  pre_raw_dir)
    purge_s3_bucket(pre_raw_bucket, pre_raw_dir)
    log_manager.log(message="S3 cleanup job is completed", args={"environment": args[REGION_KEY],"landing zone":lz_bucket+'/'+lz_feed_file_dir,"pre-raw zone":pre_raw_bucket+'/'+pre_raw_dir})


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

