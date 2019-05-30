package com.pharbers.ipaas.data.driver.libs.spark.util

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.ReadConfig
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/**
  * Created by clock on 18-2-27.
  */
case class mongo2RDD(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    def mongo2RDD(mongodbHost: String,
                  mongodbPort: String,
                  databaseName: String,
                  collName: String,
                  readPreferenceName: String = "secondaryPreferred"): MongoRDD[Document] = {
        val readConfig = ReadConfig(Map(
            "spark.mongodb.input.uri" -> s"mongodb://$mongodbHost:$mongodbPort/",
            "spark.mongodb.input.database" -> databaseName,
            "spark.mongodb.input.collection" -> collName,
            "readPreference.name" -> readPreferenceName)
        )
        MongoSpark.load(conn_instance.sc, readConfig = readConfig)
    }
}