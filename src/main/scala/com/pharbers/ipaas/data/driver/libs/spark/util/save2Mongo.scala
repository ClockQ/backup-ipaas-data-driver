package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/** SPARK 常用工具集，保存 DataFrame 数据到 MongoDB
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class save2Mongo(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {

    /** 保存 DataFrame 数据到 MongoDB
      *
      * @param dataFrame 要保存的数据集
      * @param mongodbHost mongodb 连接地址
      * @param mongodbPort mongodb 连接端口
      * @param databaseName 保存的数据库名
      * @param collName 保存的集合名
      * @param saveMode 保存模式
      * @return Unit
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:23
      * @example 默认参数例子
      * {{{
      * save2Mongo(df, "mongodb_host", "27017", "test_db", "test_coll", SaveMode.Append)
      * }}}
      */
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