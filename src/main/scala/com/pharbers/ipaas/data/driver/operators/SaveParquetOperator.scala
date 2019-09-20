package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.Annotation.Operator
import org.apache.spark.sql.{Column, SaveMode}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.save2Parquet
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhOperatorTrait, PhPluginTrait, PhSparkDriverArgs, PhStringArgs, PhWorkArgs}

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
@Operator(args = Array("path", "saveMode"), msg = "save df to hdfs as parquet", name = "save_parquet")
case class SaveParquetOperator(name: String,
                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                               pluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhOperatorTrait[String] {

    /** spark driver 实例 */
    val sd: PhSparkDriver = ctx.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get

    /** 要存储的DataFrame */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get

    /** 要保存的路径地址 */
    val path: String = defaultArgs.getAs[PhStringArgs]("path").get.get
    /** 要保存的方式 */
    val saveMode: SaveMode = defaultArgs.getAs[PhStringArgs]("saveMode") match {
        case Some(one) => one.get.toUpperCase() match {
            case "OVERWRITE" => SaveMode.Overwrite
            case _ => SaveMode.Append
        }
        case None => SaveMode.Append
    }

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[String] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        sd.setUtil(save2Parquet()(sd)).save2Parquet(inDF, path, saveMode)
        PhStringArgs(path)
    }
}
