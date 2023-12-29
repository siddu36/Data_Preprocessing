#This job copy files from landing zone to preraw zone
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
    print('starting..')
    print('Config file prefix : ', args[FEED_CONFIG_FILE_KEY])
    guid = args[GUID_KEY]
    batch_date = args[BATCH_DATE_KEY]
    config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    # Read config file
    lz_bucket = sys_config.get(args[REGION_KEY], 'lz_bucket')
    pre_raw_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    # pyspark_job = sys_config.get(args[REGION_KEY], 'pyspark_job')
    BOTO3_AWS_REGION = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    prerawtbname = config.get('db_info', 'source_table')
    client = boto3.client('logs', region_name=BOTO3_AWS_REGION)

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=prerawtbname+'_'+guid,
                                             process_key=PROCESS_KEY,client=client,job=args[REGION_KEY]+'-isg-ie-input-preprocessor')

    lz_feed_file_dir = config.get('landing_zone_info', 'lz_file_prefix')
    pre_raw_dir = config.get('landing_zone_info', 'pre_raw_file_prefix')
    data_file_extension = config.get('landing_zone_info', 'lz_data_file_extension')
    control_file_extension = config.get('landing_zone_info', 'lz_control_file_extension')
    log_manager.log(message="Starting the preprocessor job", args={"environment": args[REGION_KEY]})

    encoding = 'utf-8'
    
    data_file_list = get_files(data_file_extension, lz_bucket, lz_feed_file_dir)
    for i in range(len(data_file_list)):
        data_file_list[i] = str(data_file_list[i], encoding)
    
    control_files = get_files(control_file_extension, lz_bucket, lz_feed_file_dir)
    for i in range(len(control_files)):
        control_files[i] = str(control_files[i], encoding)
    
    log_manager.log(message="Data files to be processed", args={"data_file_list": data_file_list, "batch_date": batch_date})
    log_manager.log(message="Corresponding control files", args={"ctl_file_list": control_files})
    
    #copy data files to pre-raw location
    for data_file in data_file_list:
        filename = os.path.basename(data_file)
        print("pre raw location : ", pre_raw_bucket + "/" + pre_raw_dir + filename)
        copy_file(lz_bucket, pre_raw_bucket, lz_feed_file_dir + data_file, pre_raw_dir + filename)
        log_manager.log(message="Files moved to pre-raw zone")
        


def get_datepart(control_files):
    str_file_date_part = control_files[0].split("_")[0]
    dt_file_dt_part = datetime.strptime(str_file_date_part, "%Y%m%d")
    return dt_file_dt_part


# copy file - within same bucket
# source and target file/object provided with prefix
def copy_file(lz_bucket, pre_raw_bucket, source_file, target_file):
    s3 = boto3.resource('s3', region_name=BOTO3_AWS_REGION)
    copy_source = {
        'Bucket': lz_bucket,
        'Key': source_file
    }
    target_bucket = s3.Bucket(pre_raw_bucket)
    target_bucket.copy(copy_source, target_file)
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


def get_files(extension, bucket, dir_prefix):
    file_list = []
    s3 = boto3.resource('s3')
    mybucket = s3.Bucket(bucket)
    objs = mybucket.objects.filter(Prefix=dir_prefix)
    print('bucket object is :' + str(objs))
    for obj in objs:
        if obj.key.endswith(extension) or obj.key.endswith(extension+".gz"):
            file_list.append(os.path.basename(obj.key.encode('ascii', 'ignore')))
    print("file list",file_list)
    return file_list


def invoke_spark_job(pyspark_job, args, dt_file_dt_part, guid):
    print('invoking Glue Job...')
    glue_client = boto3.client('glue')
    response = glue_client.start_job_run(
        JobName=pyspark_job,
        Arguments={
            '--guid': guid,
            '--config_bucket': args[CONFIG_BUCKET_NAME_KEY],
            '--feed_config_file': args[FEED_CONFIG_FILE_KEY],
            '--sys_config_file': args[SYS_CONFIG_KEY],
            '--region': args[REGION_KEY],
            '--year': str(dt_file_dt_part.year),
            '--month': str(dt_file_dt_part.month)
        })


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

