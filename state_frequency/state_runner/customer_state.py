
# Local Imports
from state_frequency.utils.state_logger import StateLogger
from state_frequency.schema.customer_schema import CustomerSchema
from state_frequency.constants.consts import Constants
from state_frequency.lib.states_analyzer import get_all_states, state_value
from state_frequency.lib.state_frequencies import probability
from state_frequency.utils.parser.create_spark_session import get_spark_context, get_spark_sqlcontext

logger = StateLogger(__name__)

class CustomerState(Constants):

    def run_state(self):

        spark_context = get_spark_context()
        hivecontext = get_spark_sqlcontext(spark_context)


        # data = spark.read.schema(CustomerSchema.customer_state_schema()).format("csv").load(path="/markov/customer/")

        logger.info("Reading files from HDFS is Done ...............................................................")

        """
        Read data from HDFS and remove empty rows
        """
        data_rdd = spark_context.textFile(Constants._INPUT_PATH).filter(lambda x: x)

        """
        Get header from data.
        """
        header = data_rdd.first()

        """
        1.Filter one, it removes header from data.
        2.apply state_value function to get key value pair (see method)
        3.added all the values based each Customer Id as key.
        4.Get all states of each Customer.
        5.Get each state probabiltiy of frequency to occuar.
        6.Sort by Customer Id as a Key.
        """
        rest_rdd = data_rdd.filter(lambda t: t != header).map(state_value) \
        .reduceByKey(lambda k1,k2: k1+k2) \
        .map(lambda (k,v): get_all_states(k, v))\
        .map(lambda (k, list): probability(list)) \
        .flatMap(lambda x: x) \
        .reduceByKey(lambda v1,v2: v1 +v2)

        """
        Save the result RDD in given HDFS Path.
        """
        # rest_rdd.saveAsTextFile(Constants._RESULT_PATH)

        # from pyspark.sql.types import Row

        """
        Connvert RDD to Dadaframes and as a compression format (parqet)
        """
        # df = hivecontext.createDataFrame(rest_rdd)
        #
        # df.show(truncate=False)

        """
        Its Display the ten results
        """
        list = rest_rdd.take(10)

        for el in list:
            print el

        # data.printSchema()

        spark_context.stop()


if __name__ == '__main__':
    state = CustomerState()
    state.run_state()
