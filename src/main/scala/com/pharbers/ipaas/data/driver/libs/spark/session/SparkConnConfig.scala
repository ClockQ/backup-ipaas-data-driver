package com.pharbers.ipaas.data.driver.libs.spark.session

/**
  * Created by clock on 19-5-30
  */
private[spark] object SparkConnConfig {
    val configPath: String = "pharbers_config/spark-config.xml"

    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"
    val yarnResourceHostname: String = "spark.master"
    val yarnResourceAddress: String = "spark.master:8032"
    val yarnDistFiles: String = "hdfs://spark.master:9000/config"
    val executorMemory: String = "2g"
}