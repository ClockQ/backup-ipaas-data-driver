package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/**
  * Created by clock on 19-3-29.
  */
case class readMongo(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    def readMongo(mongodbHost: String,
                  mongodbPort: String,
                  databaseName: String,
                  collName: String,
                  readPreferenceName: String = "secondaryPreferred"): DataFrame = {
        conn_instance.ss.read.format("com.mongodb.spark.sql.DefaultSource")
                .option("spark.mongodb.input.uri", s"mongodb://$mongodbHost:$mongodbPort/")
                .option("spark.mongodb.input.database", databaseName)
                .option("spark.mongodb.input.collection", collName)
                .option("readPreference.name", readPreferenceName)
                .load()
    }
}