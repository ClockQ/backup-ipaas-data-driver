package com.pharbers.ipaas.data.driver.libs.spark.util

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/** SPARK 常用工具集，读取 Parquet 数据到 DataFrame
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class readParquet(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {

    /** 读取 Parquet 数据到 DataFrame
      *
      * @param file_path 读取的文件路径
      * @param header 是否处理头部
      * @return _root_.org.apache.spark.sql.DataFrame 读取成功的数据集
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:26
      * @example 默认参数例子
      * {{{
      * readParquet("hdfs:///test/read", true)
      * }}}
      */
    def readParquet(file_path: String, header: Boolean = true): DataFrame = {
        conn_instance.ss.read.option("header", header.toString).parquet(file_path)
    }
}