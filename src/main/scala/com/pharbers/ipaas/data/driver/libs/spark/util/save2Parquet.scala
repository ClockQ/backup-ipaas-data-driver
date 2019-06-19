package com.pharbers.ipaas.data.driver.libs.spark.util

import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance
import org.apache.spark.sql.{DataFrame, SaveMode}

/** 功能描述
  *
  * @author EDZ
  * @param
  * @tparam
  * @note
  */
case class save2Parquet(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    def save2Parquet(dataFrame: DataFrame,
                   location: String,
                   saveMode: SaveMode = SaveMode.Append): Unit = {
        dataFrame.write.mode(saveMode)
                .option("header", value = true)
                .parquet(location)
    }
}
