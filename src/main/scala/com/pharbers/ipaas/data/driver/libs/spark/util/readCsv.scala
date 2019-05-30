package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/**
  * Created by clock on 18-2-27.
  */
case class readCsv(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    def readCsv(file_path: String
                , delimiter: String = ","
                , header: Boolean = true): DataFrame = {
        conn_instance.ss.read.format("com.databricks.spark.csv")
                .option("header", header)
                .option("delimiter", delimiter)
                .load(file_path)
    }
}