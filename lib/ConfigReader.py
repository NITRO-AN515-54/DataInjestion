import configparser
from pyspark import SparkConf

# loading the application configs in python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("configs/application.conf")
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf

# loading the pyspark configs and creating a spark conf object
def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)

    # Set the JDBC driver path
    pyspark_conf.set("spark.jars",
                     "C:\\Users\\e140856\\OneDrive - Mastercard\\Documents\\Office work\\Python To Report server\\DataInjestion\\dbJars\\mssql-jdbc-12.8.1.jre8.jar, C:\\Users\\e140856\\OneDrive - Mastercard\\Documents\\Office work\\Python To Report server\\DataInjestion\\dbJars\\ojdbc17.jar")

    return pyspark_conf