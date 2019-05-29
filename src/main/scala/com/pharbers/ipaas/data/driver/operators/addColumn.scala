package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:21
  */
case class addColumn[A <: PhWorkArgs[_]](override val name: String = "addColumn",
                                         override val defaultArgs: PhWorkArgs[_] = PhNoneArgs) extends PhOperatorTrait {

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
        val inDFName = prMapArgs.getAs[PhStringArgs]("inDFName").get.get
        val outDFName = prMapArgs.getAs[PhStringArgs]("outDFName") match {
            case Some(one) => one.get
            case None => inDFName
        }
        val newColName = prMapArgs.getAs[PhStringArgs]("newColName").get.get
        val funcName = prMapArgs.getAs[PhStringArgs]("funcName").get.get

        val inDF = prMapArgs.getAs[PhDFArgs](inDFName).get.get

        val func = prMapArgs.getAs[PhFuncArgs](funcName).get.get(prMapArgs).toColArgs.get

        val outDF = inDF.withColumn(newColName, func)
        outDF.show(false)
        PhMapArgs(
            prMapArgs.get + (outDFName -> PhDFArgs(outDF))
        )
    }
}
