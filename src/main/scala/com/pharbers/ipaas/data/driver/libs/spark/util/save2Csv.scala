package com.pharbers.ipaas.data.driver.libs.spark.util

import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance
import org.apache.spark.sql.DataFrame

/** 将DataFrame以csv的形式保存到HDFS的一个路径
  *
  * @author cui
  * @version 0.1
  * @since 2019-06-27 18:47
  */
case class save2Csv()(implicit val conn_instance: SparkConnInstance) extends SparkUtilTrait {
	/** 将DataFrame以csv的形式保存到HDFS的一个路径
	  *
	  * @param df 要保存的DataFrame
	  * @param path HDFS上的路径
	  * @param delimiter avro 文件路径
	  * @return Unit
	  * @author cui
	  * @version 0.1
	  * @since 2019-06-27 18:47
	  * @example 默认参数例子
	  * {{{
	  *     df: DataFrame // 要保存的 DataFrame
	  *     path: String // HDFS上的路径
	  *     delimiter: String // 分隔符，默认为 "#"
	  * }}}
	  */
	def save2Csv(df: DataFrame, path: String, delimiter: String = "#"): Unit ={
		df.coalesce(1).write
			.format("csv")
			.option("encoding", "UTF-8")
			.option("header", value = true)
			.option("delimiter", delimiter)
			.save(path)
	}
}
