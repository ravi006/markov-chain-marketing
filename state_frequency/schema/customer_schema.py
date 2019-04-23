
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DoubleType


class CustomerSchema():

    @staticmethod
    def customer_state_schema():
        return StructType([
            StructField("cust_id", IntegerType(), False),
            StructField("trans_id", IntegerType(), False),
            StructField("trans_date", DateType(), False),
            StructField("amount", DoubleType(), False)
        ])