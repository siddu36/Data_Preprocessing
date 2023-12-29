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

# logging.basicConfig(level=logging.INFO)
# logging.basicConfig(format='%(message)s')
DEFAULT_NUM_PARTITIONS = '30'
CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = 'batch_date'
PROCESS_KEY = 'landing_to_raw'
NUM_PARTS = "num_partitions"


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
    pre_raw_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    client = boto3.client('logs', region_name=BOTO3_AWS_REGION)

    pre_raw_table = config.get('db_info', 'source_table')
    base_table = config.get('db_info', 'target_table')
    history_table = config.get('db_info', 'history_table')
    raw_zone_table_path = config.get('db_info', 'rz_table_path')
    hist_table_path = config.get('db_info', 'hist_table_path')
    primary_key = config.get('db_info', 'primary_key')
    incrementalloadflag = config.get('landing_zone_info', 'incrementalload')
    doschemavalidation = config.get('landing_zone_info', 'schemavalidation', fallback='true')
    data_file_ext = config.get('landing_zone_info', 'lz_data_file_extension')
    pre_raw_prefix = config.get('landing_zone_info', 'pre_raw_file_prefix')
    delimiter = config.get('landing_zone_info', 'delimiter', fallback='null')



    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=pre_raw_table+'_'+guid,
                                             process_key=PROCESS_KEY, client=client,job=args[REGION_KEY]+'-isg-ie-ingest-into-raw')
    log_manager.log(message="Starting the ingest-to-raw job",
                    args={"environment": args[REGION_KEY], "feed": pre_raw_table, "Numpartitions": numpartitions})


    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    # logger = glueContext.get_logger()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    incremental_data_df = spark.sql("select * from " + tempdb + "." + pre_raw_table)

    # logger.info("after validation - logging",schemavalidation)
    # log("Schema validation complete ", cloudwatch_log_group, guid)


    # 1. Append incremental data to base table
    # test_query="select * from " + database + "." + pre_raw_table
    
    print("bucket name: " + raw_zone_bucket + " , folder name: " + raw_zone_table_path)
    df_base_cnt = spark.sql('select count(*) basecnt from ' + database + '.' + base_table)
    prior_raw_cnt = (df_base_cnt.select("basecnt").collect()[0]).asDict()
    for value in prior_raw_cnt.values():
        prior_raw_count = int(value)
        print("base table count :" + str(prior_raw_count))
        log_manager.log(message="base table count", args={"count": prior_raw_count})

    # purge landing zone location
    print("bucket name: " + raw_zone_bucket + " , folder name: " + raw_zone_table_path)
    purge_s3_bucket(raw_zone_bucket, raw_zone_table_path)
    inc_data_to_base_sql = 'insert overwrite table ' + database + '.' + base_table + ' select /*+ COALESCE(' + numpartitions + ') */ *,\'' + batch_run_date + '\' from ' + tempdb + '.' + pre_raw_table
    print('inc_data_to_base_sql: ' + inc_data_to_base_sql)
    log_manager.log(message='Append incremental data to base table',
                    args={"feed": pre_raw_table, 'inc_data_to_base_sql': inc_data_to_base_sql})

    try:
        spark.sql(inc_data_to_base_sql)
    except AnalysisException as e:
        if 'Can not create a Path from an empty string' in str(e):
            print('Caught analysis exception in write to raw table...' + str(e))
            log_manager.log(message='Caught analysis exception in write to raw table', args={'exception': str(e)})
        else:
            print('Caught analysis exception in write to raw table...' + str(e))
            log_manager.log_error(message='Caught analysis exception in write to raw table', args={'exception': str(e)})
            sys.exit(str(e))
    log_manager.log(message="incremental load flag", args={"flagvalue": incrementalloadflag})
    if incrementalloadflag == 'true':
        print("Incremetal flag is true")

        # 2. Overwrite the base table with the unchanged data
        log_manager.log(message="primarykey", args={"primarykey": primary_key})
        pk_str = str(primary_key)
        whrclause = ''
        pklist = []
        pklist = pk_str.split(',')
        log_manager.log(message="pklist", args={"pklist": pklist})
        for i, item in enumerate(pklist):
            if i:
                whrclause = (whrclause + ' and ')
            whrclause = (whrclause + 'L.' + item.strip() + ' = S.' + item.strip())
        log_manager.log(message="pk where clause", args={"clause": whrclause})
        df_unchanged_cnt = spark.sql('select count(*) unchangedcnt from ' + database + '.' + history_table + ' L Left Outer Join ' + database + '.' + base_table + ' S on '+whrclause +' where L.batchdate = (select max (batchdate) from ' + database + '.' + history_table +') and S.'+ item.strip()+ ' is null')
        unchanged_cnt = {}
        unchanged_cnt = (df_unchanged_cnt.select("unchangedcnt").collect()[0]).asDict()
        for value in unchanged_cnt.values():
            unchanged_rec_count = int(value)
            log_manager.log(message="unchanged rec count", args={"count": unchanged_rec_count})
            print('unchanged_rec_count: ' + str(unchanged_rec_count))
        unchanged_rec_sql = 'select /*+ COALESCE('+ numpartitions +') */ L.* from ' + database + '.' + history_table + ' L Left Outer Join ' + database + '.' + base_table + ' S on '+whrclause +' where L.batchdate = (select max (batchdate) from ' + database + '.' + history_table +') and S.'+ item.strip()+ ' is null'
        print('unchanged_rec_sql: ' + unchanged_rec_sql)
        log_manager.log(message="unchanangedrecsql", args={"sqltext": unchanged_rec_sql})
        if unchanged_rec_count > 0:
            try:
                unchanged_data_df = spark.sql(unchanged_rec_sql)
            except AnalysisException as e:
                if 'Can not create a Path from an empty string' in str(e):
                    log_manager.log(message="Anaysis exception in left outer join", args={"exception": str(e)})
                    print('Caught analysis exception in data fetch of unchanged records...' + str(e))
                else  :
                    log_manager.log_error(message="Anaysis exception in left outer join", args={"exception": str(e)})
                    print('Caught analysis exception in data fetch of unchanged records...' + str(e))
                    sys.exit(str(e))
            unchanged_data_df.registerTempTable("unchangedRecordsTable")
            write_to_hist_sql = "insert into " + database + "." + base_table + " select /*+ COALESCE(" + numpartitions + ") */ * from unchangedRecordsTable"
            log_manager.log(message="Unchnaged record copy query ", args={"sqltext:": write_to_hist_sql})
            try:
                spark.sql(write_to_hist_sql)
            except AnalysisException as e:
                if 'Can not create a Path from an empty string' in str(e):
                    log_manager.log(message="Analysis exception in write unchanged to raw ",
                                    args={"errortext": str(e)})
                    print('Caught analysis exception...' + str(e))
                else  :
                    log_manager.log_error(message="Analysis exception in write unchanged to raw ",
                                    args={"exception": str(e)})
                    print('Caught analysis exception...' + str(e))
                    sys.exit(str(e))

    # 3. Move full snapshot from base table to new partition in history
    try:
        df_base_cnt = spark.sql('select count(*) basecnt from ' + database + '.' + base_table)
        base_cnt = {}
        base_cnt = (df_base_cnt.select("basecnt").collect()[0]).asDict()
        for value in base_cnt.values():
            base_table_count = int(value)
            #print("base table count :" + str(base_table_count))
            #log_manager.log(message="base table count", args={"count": base_table_count})
        base_table_df = spark.sql('select /*+ COALESCE(' + numpartitions + ') */ * from ' + database + '.' + base_table)
        #  base_table_count = base_table_df.count()
        if base_table_count > 0:
            # partitiondate = base_table_df.first()['batchdate']
            partitiondate = base_table_df.select("batchdate").rdd.max()[0]
        else:
            partitiondate = '2001-01-31'
        log_manager.log(message="Partition Date", args={"partitiondate": partitiondate})
        base_table_df = base_table_df.drop("batchdate")
        base_table_df.registerTempTable("temp_base_table");
        base_to_partition_sql = 'insert overwrite table ' + database + '.' + history_table + ' partition (batchdate=\'' + partitiondate + '\') select /*+ COALESCE(' + numpartitions + ') */ * from  temp_base_table'
        print('base_to_partition_sql: ' + base_to_partition_sql)
        log_manager.log(message="Query to move full snapshot from raw table to new partition in history",
                        args={"sqltext": base_to_partition_sql})
        spark.sql(base_to_partition_sql)
    except AnalysisException as e:
        if 'Can not create a Path from an empty string' in str(e):
            print('Caught analysis exception to move full snapshot from raw table to new partition in history...' + str(e))
            log_manager.log(message='Caught analysis exception to move full snapshot from raw table to new partition in history.', args={'exception': str(e)})
        else:
            print('Caught analysis exception to move full snapshot from raw table to new partition in history...' + str(e))
            log_manager.log_error(message='Caught analysis exception', args={'exception': str(e)})
            sys.exit(str(e))
#    try:
#        df_raw_cnt=spark.sql("select batchdate,count(*) as cnt from "+ database + "." + base_table +" group by batchdate")
#        row_list = df_raw_cnt.collect()
#        for row in row_list:
#            if batch_run_date == row["batchdate"]:
#                log_manager.log(message="Delta Count Raw", args={"count": row["cnt"]})
#            else:
#                log_manager.log(message="Unchanged Count Raw", args={"count": row["cnt"]})
#    except AnalysisException as e:
#        print('Caught analysis exception...' + str(e))



    job.commit()
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