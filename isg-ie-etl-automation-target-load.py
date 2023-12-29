#automated data gatering from ISGIE as per the request id and columns we get from business
import sys
from pyspark.sql import DataFrameWriter
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
from pyspark.sql import SQLContext
import configparser
from io import StringIO
import os
from awsglue.job import Job
import boto3
import time
import re
import logging_service

CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
REQUEST_ID = "request_id"
ACCESSZONE_ID = "access_zone_id"


def main():
    global BOTO3_AWS_REGION
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               REQUEST_ID,
                               ACCESSZONE_ID,
                               REGION_KEY])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    guid = args[GUID_KEY]
    region = args[REGION_KEY]
    sys_config_key = args[SYS_CONFIG_KEY]
    config_bucket_name = args[CONFIG_BUCKET_NAME_KEY]
    accesszone_id = args[ACCESSZONE_ID]
    request_id = args[REQUEST_ID]

    athena_raw_bucket = 'pruvpcaws003-isg-ie-dev'
    workgroup = 'isg-sunrise'
    database = 'dev_isg_ie_sunrise'
    
    target_table = accesszone_id + '_' + request_id + '_iris'
    target_table = target_table.replace('-','_')

    datareq_table = 'isg_ie_datarequest_accesszone'
    datareq_query_table = 'isg_ie_datareq_querycol_v2'

    s3_location, access_database = get_access(accesszone_id, database, datareq_table, spark)
    res = create_table(access_database, target_table, s3_location, region, workgroup, request_id,
                       datareq_query_table, spark)

    client_s3 = boto3.client('s3')
    sqlfile = request_id +'_query'+ '.sql'
    sql_key = 'dynamic-etl/app/etl_queries/' + sqlfile

    filename = client_s3.get_object(Bucket=athena_raw_bucket, Key=sql_key)
    query = filename['Body'].read().decode('utf-8')
    query = query.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    query = re.sub('{athena_raw_db}', database, query)
    print(query)
    results = spark.sql(query)
    results.show()
    print(results)
    results.registerTempTable("business_request")
    business_write = spark.sql(
        "insert overwrite table " + database + "." + target_table + " select * from business_request")
    business_write.show()


def get_access(accesszone_id, database, datareq_table, spark):
    loc_query = "select s3_location, database_name from " + database + '.' + datareq_table + " where accesszone_id = '" + accesszone_id + "'"
    resdf = spark.sql(loc_query)
    res_list = resdf.collect()
    for row in res_list:
        s3_location = row["s3_location"]
        access_database = row["database_name"]

    return s3_location, access_database


def create_table(database, tablename, s3_location, region, workgroup, requestid, datareq_query_table,
                 spark):
    selectquery = "select requestecol,data_type as dtype from " + database + '.' + datareq_query_table + " where requestid = '" + requestid + "'"
    selectdf = spark.sql(selectquery)
    row_list = selectdf.collect()
    table_columns = ""
    for row in row_list:
        column_name = row["requestecol"]
        typ = row["dtype"]
        table_columns = table_columns + column_name + ' ' + typ + ',' + '\n \t'
        print(table_columns)
    table_columns = table_columns[:-4]
    drptblstmt = 'DROP TABLE IF EXISTS ' + tablename + ';' + '\n'
    crttblstmt = 'CREATE EXTERNAL TABLE IF NOT EXISTS ' + tablename + ' (' + table_columns + ')' + '\n'
    serdeclause = 'ROW FORMAT SERDE' + '\n \t' + "'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'" + '\n'
    inputformatclause = 'STORED AS INPUTFORMAT' + '\n \t' + "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'" + '\n'
    outputformatclause = 'OUTPUTFORMAT ' + '\n \t' + "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'" + '\n'
    locationclause = 'LOCATION' + '\n \t' + "'" + s3_location + "'" ';'
    ddl_statement = drptblstmt + crttblstmt + serdeclause + inputformatclause + outputformatclause + locationclause
    sqlbatch = ddl_statement.split(';')
    numsqls = len(sqlbatch)
    s3_output = 's3://pruvpcaws003-isg-ie-dev/dynamic-etl/app/query_output'

    for i in range(numsqls - 1):
        query = sqlbatch[i]
        qr = (query.replace('\n', ' ').replace('\r', ''))
        res = run_query(qr, database, s3_output, region, workgroup)



def run_query(query, database, s3_output, region, workgroup):
    workgroup = 'isg-sunrise'
    print("Database:", database)
    print("S3 output:", s3_output)
    print("Workgroup:#", workgroup)
    print("Region:", region)
    print("Query:", query)
    client = boto3.client('athena', region_name='us-east-1')
    execution_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        },
        WorkGroup=workgroup
    )

    print(execution_id['QueryExecutionId'])
    if not execution_id:
        return
    while True:
        stats = client.get_query_execution(QueryExecutionId=execution_id['QueryExecutionId'])
        print('Full returned output : ' + str(stats))
        status = stats['QueryExecution']['Status']['State']
        print(str(status))
        if status == 'RUNNING':
            time.sleep(5)
        elif status == 'QUEUED':
            time.sleep(5)
        else:
            print("job completed with status of " + status)
            break


def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config


if __name__ == '__main__':
    main()
