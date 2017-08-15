#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module parses, filters and transforms Tealium event data.
"""

import argparse
import datetime
import os
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def get_contexts(app_name):
    """
    Returns a tuple containing (spark_context, sql_context)
    """
    spark_context = pyspark.SparkContext(
        conf=pyspark.SparkConf().setAppName(app_name))

    sql_context = (SparkSession
        .builder
        .enableHiveSupport()
        .getOrCreate())
    return (spark_context, sql_context)


def load_tealium_event_dataframe(sql_context, start_date, end_date):
    """
    Returns raw tealium events in json to a pyspark dataframe.
    """

    return None


def main(argv):
    app_name = "Script to parse, filter and transform Tealium event data"

    date_parse = lambda d: datetime.datetime.strptime(date, '%Y-%m-%d')
    parser = argparse.ArgumentParser(description=app_name)
    parser.add_argument(
        '--start_date', action='store', required=True,
        type=date_parse,
        help="start date in format 'YYYY-MM-DD' (i.e. 2015-12-31)")
    parser.add_argument(
        '--end_date', action='store', required=True,
        type=date_parse,
        help="end date in format 'YYYY-MM-DD' (i.e. 2015-12-31)")
    parser.add_argument(
        '--reverse', action='store_true', default=False,
        help="iterate dates in reverse chronological order.")
    parser.add_argument(
        '--force', action='store_true', default=False,
        help="overwrite output even if exists.")

    opts = parser.parse_args(argv)
    start_date = opts.start_date
    end_date = opts.end_date

    (spark_context, sql_context) = get_contexts(app_name)
    df_events = load_tealium_event_dataframe(sql_context, start_date, stop_date)
    spark_context.stop()
    return


if __name__ == '__main__':
    main(sys.argv[1:])
