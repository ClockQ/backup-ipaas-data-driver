package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.struct
import com.pharbers.ipaas.data.driver.api.work._

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:21
  */
case class distinctByKey(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val keys = defaultMapArgs.getAs[PhStringArgs]("keys").get.get.split("#")
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get

        val columns = inDF.columns
        val sortBy = defaultMapArgs.getAs[PhStringArgs]("chooseBy") match {
            case Some(one) => one.get
            case None => columns.head
        }
        def sortFun(col: Column): Column = defaultMapArgs.getAs[PhStringArgs]("chooseFun") match {
            case Some(str) =>
                str.get match {
                    case "min" => functions.min(col)
                    case _ => functions.max(col)
                }
            case None => functions.max(col)
        }

        val outDF = inDF.groupBy(keys.head, keys.tail: _*)
                .agg(sortFun(struct(sortBy, columns.filter(_ != sortBy): _*)) as "tmp")
                .select("tmp.*")
                .select(columns.head, columns.tail: _*)

        PhDFArgs(outDF)
    }
}
