package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.ipaas.data.driver.api.work._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

class TestJoinOperator extends FunSuite with BeforeAndAfterAll {
	implicit var sd: PhSparkDriver = _
	var testDF1: DataFrame = _
	var testDF2: DataFrame = _

	override def beforeAll(): Unit = {
		sd = PhSparkDriver("test-driver")
		val tmp = sd.ss.implicits
		import tmp._

		testDF1 = List(
			("name1", "prod1", "201801", 1),
			("name2", "prod1", "201801", 2),
			("name3", "prod2", "201801", 3),
			("name4", "prod2", "201801", 4)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		testDF2 = List(
			("name1", "prod1", "201801", 1, 1),
			("name2", "prod1", "201801", 2, 2),
			("name3", "prod2", "201801", 3, 3),
			("name4", "prod2", "201801", 4, 4)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE_RANK")

		require(sd != null)
		require(testDF1 != null)
		require(testDF2 != null)
	}

	test("join dataframe") {
		val operator = JoinOperator(
			"JoinOperator",
			PhMapArgs(Map(
				"inDFName" -> PhStringArgs("inDFName"),
				"joinDFName" -> PhStringArgs("joinDFName"),
				"joinExpr" -> PhStringArgs("NAME = CHECK_NAME"),
				"joinType" -> PhStringArgs("left")
			)),
			Seq()
		)
		val result = operator.perform(PhMapArgs(Map(
			"inDFName" -> PhDFArgs(testDF1),
			"joinDFName" -> PhDFArgs(testDF2)
		)))
		val df = result.get
		assert(df.columns.length == testDF1.columns.length + testDF2.columns.length)
		assert(df.columns.contains("CHECK_VALUE_RANK"))
	}
}
