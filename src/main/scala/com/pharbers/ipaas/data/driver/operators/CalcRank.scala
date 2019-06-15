package com.pharbers.ipaas.data.driver.operators

import env.sparkObj2
import sparkObj2.ss.implicits._
import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}

case class CalcRank() extends PhOperatorTrait{
    override val name: String = "CalcRank"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val prMapArgs = pr.toMapArgs[PhWorkArgs[_]]
	    val pluginResultDF = prMapArgs.getAs[PhFuncArgs[Any, Any]]("plugin").get.get(pr).asInstanceOf[PhDFArgs].get
	    val rankColumnName = prMapArgs.getAs[PhStringArgs]("rankColumnName").get.get
	    val resultDF = sparkObj2.sqc.createDataFrame(
		    pluginResultDF.rdd.zipWithIndex.map { case (row, columnindex) => Row.fromSeq(row.toSeq :+ (columnindex + 1)) },
		    StructType(pluginResultDF.schema.fields :+ StructField(rankColumnName, LongType, false))
		)
	    PhDFArgs(resultDF)
    }
}
