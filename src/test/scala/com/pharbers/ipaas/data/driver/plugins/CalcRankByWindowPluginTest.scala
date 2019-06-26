package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class CalcRankByWindowPluginTest extends FunSuite {
	implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")

	import sparkDriver.ss.implicits._

	val partitionColumnNames = List("PROD")
	val dateColName = "DATE"
	val valueColumnName = "VALUE"
	val outputColumnName = "RESULT"

	test("CalcRankByWindow plugin") {
		val df: DataFrame = List(
			("name1", "prod1", "201701", 1),
			("name2", "prod2", "201701", 2),
			("name3", "prod1", "201801", 2),
			("name4", "prod2", "201801", 1)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201701", 1, 2),
			("name2", "prod2", "201701", 2, 1),
			("name3", "prod1", "201801", 2, 1),
			("name4", "prod2", "201801", 1, 2)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

		val growthPlugin = CalcRankByWindowPlugin("CalcRankByWindowPlugin",
			PhMapArgs(Map(
				"dateColName" -> PhStringArgs(dateColName),
				"valueColumnName" -> PhStringArgs(valueColumnName),
				"partitionColumnNames" -> PhStringArgs("DATE")
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
