package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.util.readCsv
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** 读取 CSV 的算子
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/15 17:59
  * @example 默认参数例子
  * {{{
  *       path: hdfs:///test.csv //CSV 的路径
  *       delimiter: , //CSV 的分隔符
  * }}}
  */
case class ReadCsvOperator(name: String,
                           defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                           pluginLst: Seq[PhPluginTrait2[Any]])
        extends PhOperatorTrait2[DataFrame] {

    /** CSV 的路径 */
    val path: String = defaultArgs.getAs[PhStringArgs]("path").get.get
    /** CSV 的分隔符 */
    val delimiter: String = defaultArgs.getAs[PhStringArgs]("delimiter") match {
        case Some(one) => one.get
        case _ => ","
    }

    /**
      * @note pr中需要传递 `key` 为 `sparkDriver` 的 PhSparkDriverArgs
      */
    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        implicit val sd: PhSparkDriver = pr.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get

        PhDFArgs(sd.setUtil(readCsv()).readCsv(path, delimiter))
    }
}
