package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.Annotation.Operator
import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.save2Mongo

/** 存储 mongo 的算子
 *
 * @author clock
 * @version 0.1
 * @since 2019/6/15 17:59
 * @example 默认参数例子
 * {{{
 *       mongodbHost: "127.0.0.1" //地址
 *       mongodbPort: "27017" //端口
 *       databaseName: "db" //库名
 *       collName: "coll" //表名
 *       saveMode: "append" //保存模式, append（追加），overwrite（覆盖）
 * }}}
 */
@Operator(args = Array("mongodbHost", "mongodbPort", "databaseName", "collName", "saveMode"), msg = "save df to mongoDB", name = "save_mongoDB")
case class SaveMongoOperator(name: String,
                             defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                             pluginLst: Seq[PhPluginTrait[Any]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhOperatorTrait[DataFrame] {

    /** spark driver 实例 */
    val sd: PhSparkDriver = ctx.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get

    /** 要存储的DataFrame */
    val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get

    /** 地址 */
    val mongodbHost: String = defaultArgs.getAs[PhStringArgs]("mongodbHost").get.get
    /** 端口 */
    val mongodbPort: String = defaultArgs.getAs[PhStringArgs]("mongodbPort").get.get
    /** 库名 */
    val databaseName: String = defaultArgs.getAs[PhStringArgs]("databaseName").get.get
    /** 表名 */
    val collName: String = defaultArgs.getAs[PhStringArgs]("collName").get.get
    /** 保存模式 */
    val saveMode: SaveMode = defaultArgs.getAs[PhStringArgs]("saveMode") match {
        case Some(one) => one.get.toUpperCase() match {
            case "OVERWRITE" => SaveMode.Overwrite
            case _ => SaveMode.Append
        }
        case None => SaveMode.Append
    }

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        val inDF = pr.getAs[PhDFArgs](inDFName).get.get
        sd.setUtil(save2Mongo()(sd)).save2Mongo(inDF, mongodbHost, mongodbPort, databaseName, collName, saveMode)
        PhDFArgs(inDF)
    }
}
