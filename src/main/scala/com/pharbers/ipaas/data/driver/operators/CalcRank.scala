package com.pharbers.ipaas.data.driver.operators


import com.pharbers.data.util.sparkDriver
import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}

case class CalcRank() extends PhOperatorTrait{
    override val name: String = "CalcRank"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(args: PhWorkArgs[Any]): PhWorkArgs[_] = {
	    val argsMap = args.asInstanceOf[PhMapArgs[_]]
	    val pluginResultDF = argsMap.getAs[PhFuncArgs]("plugin").get(args).asInstanceOf[PhDFArgs].get
	    val rankColumnName = argsMap.getAs[PhStringArgs]("rankColumnName").get
	    val resultDF = sparkDriver.sqc.createDataFrame(
		    pluginResultDF.rdd.zipWithIndex.map { case (row, columnindex) => Row.fromSeq(row.toSeq :+ (columnindex + 1)) },
		    StructType(pluginResultDF.schema.fields :+ StructField(rankColumnName, LongType, false))
	    )
	    resultDF.show(false)
	    PhDFArgs(resultDF)
    }
}
