from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lower, lit, upper, initcap, length, from_unixtime, substring, length, expr, \
    sequence
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
import pyspark.sql.functions as sf
##redditor = df.drop('_c0')
import getpass
import logging

username = getpass.getuser()


def main(spark):
    logging.basicConfig(level=logging.CRITICAL,
                        format='%(asctime)s:%(levelname)s:%(message)s')

    """Main ETL definition
    :return :None
    """

    # log = """"""
    # config = """"""

    # log that main ETL job is starting
    # log.warn('Job is Up-and-Running')
    # test etl job
    # create_test_data(spark)
    # execute ETL pipeline

    data = extract_task(spark)
    data_transformed = transformation_task(data)
    load_data(data_transformed)

    # log the success and terminate spark application
    # log.warn('Job is Finished')
    spark.stop()
    return None


def extract_task(spark):
    """Load data from parquet file format.
    #param spark: Spark session object.
    #return: Spark Dataframe.
    """
    spark.sql('use itv000513_reddit_db')
    red = spark.sql('select id_name as author,id as author_id,"user" as author_type,created_date from dim_redditor')
    sub = spark.sql(
        'select subreddit_name as author,id as author_id,"subreddit" as author_type,created_date from dim_subreddit')

    src_df = red.union(sub)
    return src_df


def transformation_task(src_df):
    """Tranform original dataset.

    :param_df: Input Dataframe.
    :return: Transformed DataFrame.
    """
    spark.sql('use itv000513_reddit_db')

    src_df = src_df.select(col('author_id').cast('string'),
                           col('author').cast('string').alias('author_name'),
                           col('author_type').cast('string'),
                           col('created_date').cast('string'))

    return src_df


def load_data(src_df):
    """write to table.

    :param df: DataFrame to print.
    :return: None
    """
    spark.sql('drop table if exists stg_author')
    src_df.write.saveAsTable('stg_author')
    # load into main table
    spark.sql('''insert into author table stg_author''')
    # s = spark.sql('select count(*) from dim_redditor')
    # print(s)
    return None


# entry point for Pyspark ETL Application
if __name__ == '__main__':
    # Start Spark Application and Spark Session,logger and config
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        enableHiveSupport(). \
        appName(f'{username} | srp_author_load'). \
        master('yarn'). \
        getOrCreate()

    main(spark)
