package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhOperatorTrait, PhPluginTrait, PhSparkDriverArgs, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.save2Parquet
import org.apache.spark.sql.{Column, DataFrame}

/** 将DataFrame1以parquet的格式保存到一个路径地址
  *
  * @author EDZ
  * @version 0.1
  * @since 2019/6/11 16:50
  * @example 默认参数例子
  * {{{
  *     inDFName: String // 要保存的 DataFrame 名字
  *     path: String // 要保存的路径地址
  * }}}
  */
case class SaveParquetOperator(name: String,
                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                               pluginLst: Seq[PhPluginTrait[Column]])
    extends PhOperatorTrait[DataFrame] {
	/** 要保存的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
	/** 要保存的路径地址 */
    val path: String = defaultArgs.getAs[PhStringArgs]("path").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        implicit val sd: PhSparkDriver = prMapArgs.getAs[PhSparkDriverArgs]("sparkDriver").get.get
        sd.setUtil(save2Parquet()).save2Parquet(inDF, path)
        PhDFArgs(inDF)
    }
}
