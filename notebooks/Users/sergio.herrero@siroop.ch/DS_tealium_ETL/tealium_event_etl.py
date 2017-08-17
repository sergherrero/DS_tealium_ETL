# Databricks notebook source
import pyspark.sql
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime


S3_TEALIUM_EVENT_SRC_PATH = "s3://siroop-tealium/siroop-tealium-events-full/{}/*/*.gz"
S3_TEALIUM_EVENT_DST_PATH = "s3://pthor-bi/data/warehouse/tealium_events"
  
def extract_tealium_event_dataframe_for_date(sql_context, target_date):
  rdd = (sqlContext._sc
           .textFile(S3_TEALIUM_EVENT_SRC_PATH.format(target_date.strftime('%Y/%m/%d')))
           .filter(lambda line: len(line.strip()) > 0))
  return sqlContext.read.json(rdd)


def transform_tealium_event_dataframe_for_date(sql_context, target_date, df_events):
  df_events = (df_events
               .withColumn('date',
                           F.udf(lambda x: datetime.datetime.strptime(x, '%Y%m%d'), T.DateType())(F.col('data.udo.yyyymmdd')))
               .select(F.col('post_time').alias("microtime"),
                       F.col('data.firstparty_tealium_cookies.device_id').alias("device_id"),
                       F.col('data.udo.user_agent').alias('user_agent'),
                       'date',
                       F.col('data.dom.referrer').alias('referrer'))
               .filter(F.col('date') == target_date)
               .distinct())
  return df_events

def load_tealium_event_dataframe_for_date(sql_context, target_date, df_events):
  df_events.write.parquet(S3_TEALIUM_EVENT_DST_PATH,
                          mode='append', partitionBy='date', compression='snappy')
  return


def etl_tealium_events_for_date_range(sql_context, start_date, end_date):
  target_date = start_date
  while target_date <= end_date:
    df_events = extract_tealium_event_dataframe_for_date(sql_context, target_date)
    df_events = transform_tealium_event_dataframe_for_date(sql_context, target_date, df_events)
    load_tealium_event_dataframe_for_date(sql_context, target_date, df_events)
    target_date += datetime.timedelta(days=1)
  return
  
 

# COMMAND ----------

from pyathenajdbc import connect

def execute_sql_athena(query_str):
  results = None
  conn = connect(s3_staging_dir='s3://aws-athena-query-results-827860021338-eu-west-1/',
               region_name='eu-west-1')
  try:
      with conn.cursor() as cursor:
          results = cursor.execute(query_str)
  finally:
      conn.close()
  return results

# COMMAND ----------

import datetime

etl_tealium_events_for_date_range(sqlContext, datetime.date(2017, 8, 4), datetime.date(2017, 8, 4))
# Make athena aware of the new partitions.
execute_sql_athena("MSCK REPAIR TABLE pthor_logs.tealium_events;")

