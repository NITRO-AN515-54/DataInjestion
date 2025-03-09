from lib import ConfigReader


def connect_to_sql(spark, env):
    conf = ConfigReader.get_app_config(env)

    connection_details = {
        "user": conf["sqlserver.datasource.username"],
        "password": conf["sqlserver.datasource.password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    df = spark.read.jdbc(url=conf["sqlserver.datasource.url"], table="OR_PackageExecutionLogs", properties=connection_details)
    return df
