from lib import ConfigReader


def connect_to_oracle(spark, env):
    conf = ConfigReader.get_app_config(env)

    connection_details = {
        "user": conf["oracle.datasource.username"],
        "password": conf["oracle.datasource.password"],
        "driver": "oracle.jdbc.OracleDriver",
    }

    df = spark.read.jdbc(url=conf["oracle.datasource.url"], table="SIAF_RPT", properties=connection_details)
    return df
