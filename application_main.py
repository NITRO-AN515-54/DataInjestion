import sys
from datasourceConfig.SQLserver import connect_to_sql
from datasourceConfig.Oracle import connect_to_oracle
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *

if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")

    spark = Utils.get_spark_session(job_run_env)

    connect_to_sql(spark, job_run_env)

    print("SQL server connection established")

    connect_to_oracle(spark, job_run_env)

    print("Oracle server connection established")

    orders_df = DataReader.read_orders(spark,job_run_env)

    orders_filtered = DataManipulation.filter_closed_orders(orders_df)

    customers_df = DataReader.read_customers(spark,job_run_env)

    joined_df = DataManipulation.join_orders_customers(orders_filtered,customers_df)
    
    aggregated_results = DataManipulation.count_orders_state(joined_df)
    
    aggregated_results.show()

    print("end of main")