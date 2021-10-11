from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lower, lit, upper, initcap, length, from_unixtime, substring, length, expr, \
    current_date
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
    df = spark.read.csv('/user/itv000513/reddit_all/*redditor*', header=True)

    return df


def transformation_task(df):
    """Tranform original dataset.

    :param_df: Input Dataframe.
    :return: Transformed DataFrame.
    """
    redditor = df.drop('_c0')
    redditor_df = redditor.withColumn('created_date', from_unixtime('created_utc', 'yyyy-MM-dd')).select(col('id'),
                                                                                                         expr(
                                                                                                             "substring(user_name,3,length(user_name))").alias(
                                                                                                             'user_name'),
                                                                                                         col(
                                                                                                             'name').alias(
                                                                                                             'id_name'),
                                                                                                         col(
                                                                                                             'over18').cast(
                                                                                                             'int').alias(
                                                                                                             "over18"),
                                                                                                         col(
                                                                                                             'profile_img').alias(
                                                                                                             "profile_img"),
                                                                                                         col(
                                                                                                             'subreddit_type').alias(
                                                                                                             "subreddit_type"),
                                                                                                         col(
                                                                                                             'subscribers').alias(
                                                                                                             "subscribers"),
                                                                                                         col(
                                                                                                             'url').alias(
                                                                                                             "url"),
                                                                                                         col(
                                                                                                             'user_is_contributor').cast(
                                                                                                             'int'),
                                                                                                         col(
                                                                                                             'user_is_moderator').cast(
                                                                                                             'int'),
                                                                                                         col(
                                                                                                             'user_is_subscriber').cast(
                                                                                                             'int'),
                                                                                                         col(
                                                                                                             'created_date').alias(
                                                                                                             "created_date"))

    ##Prepare dataframes
    spark.sql('use itv000513_reddit_db')
    ##create staging table
    spark.sql('drop table if exists stg_redditor')
    redditor_df.write.saveAsTable('stg_redditor')
    mainDF = spark.sql('select * from dim_redditor')
    delta = redditor_df.withColumn('updated_date', current_date())

    ##joining dataframes
    main = mainDF.alias('main')
    delta = delta.alias('delta')
    updatedDF = main. \
        join(delta, main.id == delta.id, 'outer')
    upsertDF = updatedDF.where((~col("main.id").isNull()) & (~col("delta.updated_date").isNull())).select(
        "delta.*").distinct()
    unchangedDF = updatedDF.where(col("main.id").isNull()).select("delta.*")
    ##delta= redditor_df.withColumn('updated_date',lit(None).cast('string'))
    unchangedDF = unchangedDF.withColumn('updated_date', lit(None).cast('string'))
    finalDF = upsertDF.union(unchangedDF)
    return finalDF


def load_data(finalDF):
    """write to table.

    :param df: DataFrame to print.
    :return: None
    """
    finalDF.createOrReplaceTempView('temp_finaldf')
    spark.sql('''insert OVERWRITE TABLE dim_redditor SELECT * FROM  temp_finaldf''')
    s = spark.sql('select count(*) from dim_redditor')
    print(s)
    return None


# entry point for Pyspark ETL Application
if __name__ == '__main__':
    # Start Spark Application and Spark Session,logger and config
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        enableHiveSupport(). \
        appName(f'{username} | srp_delta'). \
        master('yarn'). \
        getOrCreate()

    main(spark)
