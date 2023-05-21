from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import logging
import sys

# настройки логирования
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.captureWarnings(True)

# текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

kafka_bootstrap_servers = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'

kafka_user = 'kafka-admin'
kafka_pass = 'de-kafka-admin-2022'

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{}" password="{}";'.format(kafka_user, kafka_pass)
}

postgresql_settings = {
    'user': 'jovyan',
    'password': 'jovyan',
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.subscribers_feedback',
}

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages_arr = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]

spark_jars_packages = ",".join(spark_jars_packages_arr) 


TOPIC_NAME_IN = 'student.topic.cohort8.dmitriy_malyutin_in'
TOPIC_NAME_OUT = 'student.topic.cohort8.dmitriy_malyutin_out'

logging.info('=======================================================')
logging.info('current_timestamp_utc: {}'.format(current_timestamp_utc))

# создание spark-сессии
def create_spark_session():
    return SparkSession.builder \
        .config("spark.jars.packages", spark_jars_packages) \
        .config("spark.sql.session.timeZone", "UTC") \
        .appName('practicum_project_8_Malyutin') \
        .getOrCreate()

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):

    # сохраняем df в памяти
    df.persist() 
    
    # Добавляем поле 'feedback'
    feedback_df = df.withColumn('feedback', 
                                F.lit(None).cast(StringType())) 
   
    # Отправляем в postgres
    feedback_df.write \
        .format('jdbc') \
        .mode('append') \
        .options(**postgresql_settings) \
        .save()
   
    # создаём df для отправки в Kafka. Сериализация в json.
    df_to_kafka = feedback_df \
        .select(F.to_json(F.struct(F.col('*'))).alias('value'))


    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_to_kafka.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .options(**kafka_security_options) \
        .option('topic', TOPIC_NAME_OUT) \
        .option('truncate', False) \
        .save()

    # чистим память
    df.unpersist()


def restaurant_read_stream(spark):
    df = spark.readStream \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .option('subscribe', TOPIC_NAME_IN) \
        .load()

 
    df_json = df.withColumn('key_str', F.col('key').cast(StringType())) \
        .withColumn('value_json', F.col('value').cast(StringType())) \
        .drop('key', 'value')
 
    # схема сообщения для входящего json
    msg_schema = StructType([
        StructField('restaurant_id', StringType()),
        StructField('adv_campaign_id', StringType()),
        StructField('adv_campaign_content', StringType()),
        StructField('adv_campaign_owner', StringType()),
        StructField('adv_campaign_owner_contact', StringType()),
        StructField('adv_campaign_datetime_start', LongType()),
        StructField('adv_campaign_datetime_end', LongType()),
        StructField('datetime_created', LongType())
    ])
 
    # разбираем json и фильтруем по времени старта и окончания акции
    df_filtered = df_json \
        .withColumn('key', F.col('key_str')) \
        .withColumn('value', F.from_json(F.col('value_json'), msg_schema)) \
        .drop('key_str', 'value_json') \
        .select(
            F.col('value.restaurant_id').cast(StringType()).alias('restaurant_id'),
            F.col('value.adv_campaign_id').cast(StringType()).alias('adv_campaign_id'),
            F.col('value.adv_campaign_content').cast(StringType()).alias('adv_campaign_content'),
            F.col('value.adv_campaign_owner').cast(StringType()).alias('adv_campaign_owner'), 
            F.col('value.adv_campaign_owner_contact').cast(StringType()).alias('adv_campaign_owner_contact'), 
            F.col('value.adv_campaign_datetime_start').cast(LongType()).alias('adv_campaign_datetime_start'), 
            F.col('value.adv_campaign_datetime_end').cast(LongType()).alias('adv_campaign_datetime_end'), 
            F.col('value.datetime_created').cast(LongType()).alias('datetime_created')
        )\
        .filter((F.col('adv_campaign_datetime_start') <= current_timestamp_utc) & (F.col('adv_campaign_datetime_end') > current_timestamp_utc))
 
    return df_filtered

# выбираем всех пользователей с подпиской на рестораны
def subscribers_restaurants(spark):

    df = spark.read \
        .format('jdbc') \
        .options(**postgresql_settings) \
        .load()
    
    df_deduplicate = df.dropDuplicates(['client_id', 'restaurant_id'])
    return df_deduplicate

# соединяем данные из сообщения Kafka с выбранными по restaurant_id (uuid). Добавляем время создания события.
def join(read_stream_df, subscribers_restaurant_df):

    df = read_stream_df \
        .join(subscribers_restaurant_df, 'restaurant_id') \
        .withColumn('trigger_datetime_created', F.lit(current_timestamp_utc))\
        .select(
            'restaurant_id',
            'adv_campaign_id',
            'adv_campaign_content',
            'adv_campaign_owner',
            'adv_campaign_owner_contact',
            'adv_campaign_datetime_start',
            'adv_campaign_datetime_end',
            'datetime_created',
            'client_id',
            'trigger_datetime_created')
    
    return df


if __name__ == '__main__':

    spark = create_spark_session() 
    logging.info('Session created')

    logging.info('Get restaurant strem')
    read_stream_df = restaurant_read_stream(spark)
    
    logging.info('Get subscribers')
    subscribers_restaurant_df = subscribers_restaurants(spark)

    logging.info('Get result')
    result = join(read_stream_df, subscribers_restaurant_df)
 
    logging.info('Start streaming')
    result.writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='5 seconds') \
        .start() \
        .awaitTermination()