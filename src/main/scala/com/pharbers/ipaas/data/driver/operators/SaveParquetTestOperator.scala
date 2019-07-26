package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhOperatorTrait, PhPluginTrait, PhSparkDriverArgs, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.save2Parquet
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

/** 将DataFrame以parquet的格式保存到一个路径地址
  *
  * @author EDZ
  * @version 0.1
  * @since 2019/6/11 16:50
  * @example 默认参数例子
  * {{{
  * inDFName: String // 要保存的 DataFrame 名字
  * path: String // 要保存的路径地址
  * saveMode: append // 要保存的方式 append（追加，默认），overwrite（覆盖）
  * }}}
  */
case class SaveParquetTestOperator(name: String,
                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                               pluginLst: Seq[PhPluginTrait[Column]])
    extends PhOperatorTrait[DataFrame] {
	/** 要保存的 DataFrame 名字 */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
	/** 要保存的路径地址 */
    val path: String = defaultArgs.getAs[PhStringArgs]("path").get.get
    /** 要保存的方式 */
    val saveMode: SaveMode = defaultArgs.getAs[PhStringArgs]("saveMode") match {
        case Some(one) => one.get.toUpperCase() match {
            case "APPEND" => SaveMode.Append
            case "OVERWRITE" => SaveMode.Overwrite
        }
        case None => SaveMode.Append
    }

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        implicit val sd: PhSparkDriver = pr.getAs[PhSparkDriverArgs]("sparkDriver").get.get
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        val chanelId = pr.getAs[PhStringArgs](path).get.get
        val reslPath = "hdfs:///test/testCui/kafkaTest003/" + chanelId
        sd.setUtil(save2Parquet()).save2Parquet(inDF, reslPath, saveMode)
        PhDFArgs(inDF)
    }
}
