package com.pharbers.ipaas.data.driver.libs.spark.util

import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance
import org.apache.spark.sql.{DataFrame, SaveMode}

/** SPARK 常用工具集，保存 DataFrame 数据到 Parquet
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class save2Parquet(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {

    /** 保存 DataFrame 数据到 Parquet
      *
      * @param dataFrame 要保存的数据集
      * @param location 保存位置
      * @param saveMode 保存模式
      * @return Unit
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:24
      * @example 默认参数例子
      * {{{
      * save2Parquet(df, "hdfs:///test/save", SaveMode.Append)
      * }}}
      */
    def save2Parquet(dataFrame: DataFrame,
                     location: String,
                     saveMode: SaveMode = SaveMode.Append): Unit = {
        dataFrame.write.mode(saveMode)
                .option("header", value = true)
                .parquet(location)
    }
}
