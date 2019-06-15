package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.plugin.SortPlugin
import env.sparkObj2
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TestJoinOperator extends FunSuite {
	test("join operator") {
        sparkObj2.sc.addJar("target/ipaas-data-driver-0.1.jar")
		import sparkObj2.ss.implicits._

		val df1: DataFrame = List(
			("name1", "prod1", "201801", 1),
			("name2", "prod1", "201801", 2),
			("name3", "prod2", "201801", 3),
			("name4", "prod2", "201801", 4)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val df2: DataFrame = List(
			("name1", "prod1", "201801", 1, 1),
			("name2", "prod1", "201801", 2, 2),
			("name3", "prod2", "201801", 3, 3),
			("name4", "prod2", "201801", 4, 4)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE_RANK")

		df1.withColumn("abc", expr("'asdf'")).show(false)
//		df1.join(df2, expr("$df1.NAME = $df2.CHECK_NAME"))

//		val result: DataFrame = JoinOperator(null, "join", PhMapArgs(Map(
//            "inDFName" -> PhStringArgs("df1"),
//            "joinDFName" -> PhStringArgs("df2"),
//            "joinExpr" -> PhStringArgs("NAME = CHECK_NAME"),
//            "joinType" -> PhStringArgs("left")
//		))).perform(PhMapArgs(Map(
//			"df1" -> PhDFArgs(df1),
//			"df2" -> PhDFArgs(df2)
//		))).asInstanceOf[PhDFArgs].get
//
//		result.show(false)
//		assert(result.columns.length == df1.columns.length + df2.columns.length)
	}
}
