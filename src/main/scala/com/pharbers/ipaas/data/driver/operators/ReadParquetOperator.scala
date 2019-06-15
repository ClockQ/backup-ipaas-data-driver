package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet

/** 读取 Parquet 的算子
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/15 18:59
  * @example 默认参数例子
  * {{{
  *        path: hdfs:///test //Parquet 的路径
  * }}}
  */
case class ReadParquetOperator(name: String,
                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                               pluginLst: Seq[PhPluginTrait2[Any]])
        extends PhOperatorTrait2[DataFrame] {

    /** Parquet 的路径 */
    val path: String = defaultArgs.getAs[PhStringArgs]("path").get.get

    /**
      * @note pr中需要传递 `key` 为 `sparkDriver` 的 PhSparkDriverArgs
      */
    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
        implicit val sd: PhSparkDriver = pr.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get

        PhDFArgs(sd.setUtil(readParquet()).readParquet(path))
    }
}
