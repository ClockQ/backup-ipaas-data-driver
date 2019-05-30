package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/**
  * Created by clock on 18-2-27.
  */
case class save2Mongo(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    def save2Mongo(dataFrame: DataFrame,
                 mongodbHost: String,
                 mongodbPort: String,
                 databaseName: String,
                 collName: String,
                 saveMode: SaveMode = SaveMode.Append): Unit = {
        dataFrame.write.format("com.mongodb.spark.sql.DefaultSource").mode(saveMode)
                .option("uri", s"mongodb://$mongodbHost:$mongodbPort/")
                .option("database", databaseName)
                .option("collection", collName)
                .save()
    }
}