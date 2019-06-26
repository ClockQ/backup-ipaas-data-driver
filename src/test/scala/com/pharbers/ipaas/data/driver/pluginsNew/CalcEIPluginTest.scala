package com.pharbers.ipaas.data.driver.pluginsNew

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhListArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.plugins.CalcEIPlugin
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class CalcEIPluginTest extends FunSuite {
	implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")
	import sparkDriver.ss.implicits._
	val partitionColumnNames = List("PROD")
	val dateColName = "DATE"
	val valueColumnName = "VALUE"
	val outputColumnName = "RESULT"
	test("EI plugin"){
		val df: DataFrame = List(
			("name1", "prod1", "201701", 0.25),
			("name2", "prod2", "201702", 0.5),
			("name3", "prod1", "201801", 1.0),
			("name4", "prod2", "201802", 1.0)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201701", 0.25, 0),
			("name2", "prod2", "201702", 0.5, 0),
			("name3", "prod1", "201801", 1.0, 4),
			("name4", "prod2", "201802", 1.0, 2)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")


		val growthPlugin = CalcEIPlugin("",
			PhMapArgs(Map(
				"valueColumnName" -> PhStringArgs(valueColumnName),
				"dateColName" -> PhStringArgs(dateColName),
				"partitionColumnNames" -> PhStringArgs("PROD")
			)),
			Seq()
		).perform(PhMapArgs(Map().empty)).asInstanceOf[PhColArgs].get

		val result = df.withColumn(outputColumnName, growthPlugin)
		result.show()
		assert(result.columns.contains(outputColumnName))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME") && col("CHECK_PROD") === col("PROD"))
			.filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}
}
