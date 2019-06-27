package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TestCalcYearGrowthPlugin extends FunSuite {
	implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")

	import sparkDriver.ss.implicits._

	val partitionColumnNames = List("PROD")
	val dateColName = "DATE"
	val valueColumnName = "VALUE"
	val outputColumnName = "RESULT"

	test("CalcYearGrowth plugin") {

		val df: DataFrame = List(
			("name1", "prod1", "201701", 1),
			("name2", "prod2", "201701", 2),
			("name3", "prod1", "201801", 2),
			("name4", "prod2", "201801", 3)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201701", 1, 0.0),
			("name2", "prod2", "201701", 2, 0.0),
			("name3", "prod1", "201801", 2, 1.0),
			("name4", "prod2", "201801", 3, 0.5)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

		val yearGrowthPlugin = CalcYearGrowthPlugin("CalcYearGrowthPlugin",
			PhMapArgs(Map(
				"dateColName" -> PhStringArgs(dateColName),
				"valueColumnName" -> PhStringArgs(valueColumnName),
				"partitionColumnNames" -> PhStringArgs("PROD")
			)),
			Seq()
		).perform(PhMapArgs(Map().empty)).asInstanceOf[PhColArgs].get
		val result = df.withColumn(outputColumnName, yearGrowthPlugin)
		result.show()
		assert(result.columns.contains(outputColumnName))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}
}
