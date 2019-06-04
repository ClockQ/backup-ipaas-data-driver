package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.functions._

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class JoinOperator(plugin: PhPluginTrait, name: String, defaultArgs: PhWorkArgs[_]) extends PhOperatorTrait {

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val defaultMapArgs = defaultArgs.toMapArgs[PhWorkArgs[_]]
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = defaultMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val joinDFName = defaultMapArgs.getAs[PhStringArgs]("joinDFName").get.get
        val joinExpr = defaultMapArgs.getAs[PhStringArgs]("joinExpr").get.get
        val joinType = defaultMapArgs.getAs[PhStringArgs]("joinType").get.get
        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get
        val joinDF = prMapArgs.getAs[PhDFArgs](joinDFName).get.get
        val outDF = inDF.join(joinDF, expr(joinExpr), joinType)

        PhDFArgs(outDF)
    }
}