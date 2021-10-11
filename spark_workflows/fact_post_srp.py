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
    df1 = spark.read.csv('/user/itv000513/reddit_all/*submission*', header=True)
    df2 = spark.read.csv('/user/itv000513/reddit_all/comments_2021_10_10*', header=True)
    sub = df1.withColumn('created_date', from_unixtime('created_utc', 'yyyy-MM-dd')).select(
        col('author').cast('string'),
        col('author_flair_text').cast('string'),
        col('post_text').cast('string').alias('post_text'),
        col('likes').cast('int'),
        col('subreddit').cast('string'),
        col('subreddit_id').cast('string'),
        col('parent_id').cast('string'),
        col('created_date').cast('string'),
        col('score').cast('int'),
        col('post_url').cast('string').alias('post_url')).drop('_c0')
    com = df2.withColumn('created_date', from_unixtime('created_utc', 'yyyy-MM-dd')).select(
        col('author').cast('string'),
        col('author_flair_text').cast('string'),
        col('post_text').cast('string').alias('post_text'),
        col('likes').cast('int'),
        col('subreddit').cast('string'),
        col('subreddit_id').cast('string'),
        col('parent_id').cast('string'),
        col('created_date').cast('string'),
        col('score').cast('int'),
        col('post_url').cast('string').alias('post_url')).drop('_c0')
    src_df = sub.union(com)
    return src_df


def transformation_task(src_df):
    """Tranform original dataset.

    :param_df: Input Dataframe.
    :return: Transformed DataFrame.
    """
    spark.sql('use itv000513_reddit_db')
    seed = spark.sql('select count(*) as count from fact_post').collect()
    seed = seed[0][0]
    # print(seed)

    src_df = src_df.withColumn('seq_id', seed + row_number().over(Window.orderBy(monotonically_increasing_id()))).drop(
        '_c0')

    return src_df


def load_data(src_df):
    """write to table.

    :param df: DataFrame to print.
    :return: None
    """
    spark.sql('drop table if exists stg_post')
    src_df.write.saveAsTable('stg_post')
    # load into main table
    spark.sql('''insert into fact_post table stg_post''')
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
        appName(f'{username} | srp_fact_post_load'). \
        master('yarn'). \
        getOrCreate()

    main(spark)
