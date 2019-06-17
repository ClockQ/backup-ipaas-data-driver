package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/** SPARK 常用工具集，读取 CSV 数据到 DataFrame
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class readCsv(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
    /** 读取 CSV 数据到 DataFrame
      *
      * @param file_path CSV 文件路径
      * @param delimiter CSV 分割符
      * @param header    是否处理头部
      * @return _root_.org.apache.spark.sql.DataFrame 读取成功的数据集
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:30
      * @example 默认参数例子
      *          {{{
      *           readCsv("hdfs:///test/read.csv", ",", true)
      *          }}}
      */
    def readCsv(file_path: String, delimiter: String = ",", header: Boolean = true): DataFrame = {
        conn_instance.ss.read.format("com.databricks.spark.csv")
                .option("header", header)
                .option("delimiter", delimiter)
                .load(file_path)
    }
}