package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

case class readParquet(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    def readParquet(file_path: String, header: Boolean = true): DataFrame = {
        conn_instance.ss.read.option("header", header.toString).parquet(file_path)
    }
}