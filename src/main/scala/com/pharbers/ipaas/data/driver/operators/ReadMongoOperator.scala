package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.Annotation.Operator
import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.util._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** 读取 mongo 的算子
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
 * }}}
 */
@Operator(args = Array("mongodbHost", "mongodbPort", "databaseName", "collName"), source = Array(), msg = "read mongoDB", name = "read_mongoDB")
case class ReadMongoOperator(name: String,
                             defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                             pluginLst: Seq[PhPluginTrait[Any]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhOperatorTrait[DataFrame] {

    /** spark driver 实例 */
    val sd: PhSparkDriver = ctx.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get

    /** 地址 */
    val mongodbHost: String = defaultArgs.getAs[PhStringArgs]("mongodbHost").get.get
    /** 端口 */
    val mongodbPort: String = defaultArgs.getAs[PhStringArgs]("mongodbPort").get.get
    /** 库名 */
    val databaseName: String = defaultArgs.getAs[PhStringArgs]("databaseName").get.get
    /** 表名 */
    val collName: String = defaultArgs.getAs[PhStringArgs]("collName").get.get

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        PhDFArgs(sd.setUtil(readMongo()(sd)).readMongo(mongodbHost, mongodbPort, databaseName, collName))
    }
}
