package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class SplitTakeHeadPluginTest extends FunSuite {
	implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")

	import sparkDriver.ss.implicits._

	val df: DataFrame = List(
		("name1", "prod1", "201801", List("aaa", "bbb", "ccc")),
		("name2", "prod2", "201801", List("aaa", "bbb", "ccc")),
		("name3", "prod1", "201802", List("aaa", "bbb", "ccc")),
		("name4", "prod2", "201802", List("aaa", "bbb", "ccc"))
	).toDF("NAME", "PROD", "DATE", "RESULT")

	test("SplitTakeHeadPluginTest") {

		val checkDf: DataFrame = List(
			("name1", "prod1", "201801", "aaa"),
			("name2", "prod2", "201801", "aaa"),
			("name3", "prod1", "201802", "aaa"),
			("name4", "prod2", "201802", "aaa")
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_RESULT")

		val Splitplugin = SplitTakeHeadPlugin("SplitPlugin",
			PhMapArgs(Map(
				"splitedColName" -> PhStringArgs("RESULT"),
				"df" -> PhDFArgs(df)
			)),
			Seq()
		).perform(PhMapArgs(Map().empty)).toColArgs.get
		val result = df.withColumn("RESULT", Splitplugin)
		result.cache()
		result.show(false)
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}
}
