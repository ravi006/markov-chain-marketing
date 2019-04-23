from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext

def get_spark_context():
   conf = SparkConf().setAppName("Customer State Frequencies")

   spark_context = SparkContext(conf=conf)

   return spark_context


def get_spark_sqlcontext(spark_context):
   return HiveContext(spark_context)