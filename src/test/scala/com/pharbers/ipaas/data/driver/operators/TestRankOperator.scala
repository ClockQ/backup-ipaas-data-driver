package com.pharbers.ipaas.data.driver.operators


import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.plugin.SortPlugin
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import env.sparkObj2

class TestRankOperator extends FunSuite {
	test("rank operator output must have rank column") {
        sparkObj2.sc.addJar("target/ipaas-data-driver-0.1.jar")
		val sortList = PhListArgs(List(PhStringArgs("VALUE")))
		//        val partitionColumns = List("PROD", "DATE")
		val orderStr = PhStringArgs("asc")
		val rankColumnName = PhStringArgs("VALUE_RANK")
		val plugin = PhFuncArgs(SortPlugin().perform)
		import sparkObj2.ss.implicits._
		val df: DataFrame = List(
			("name1", "prod1", "201801", 1),
			("name2", "prod1", "201801", 2),
			("name3", "prod2", "201801", 3),
			("name4", "prod2", "201801", 4)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201801", 1, 1),
			("name2", "prod1", "201801", 2, 2),
			("name3", "prod2", "201801", 3, 3),
			("name4", "prod2", "201801", 4, 4)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE_RANK")
		val result: DataFrame = CalcRank().perform(PhMapArgs(
			Map(
				"sortList" -> sortList,
				//                "partitionColumns" -> partitionColumns,
				"rankColumnName" -> rankColumnName,
				"df" -> PhDFArgs(df),
				"plugin" -> plugin,
				"orderStr" -> orderStr
			)
		)).asInstanceOf[PhDFArgs].get
		result.show(false)
		assert(result.columns.contains(rankColumnName.get))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("CHECK_VALUE_RANK") =!= col("VALUE_RANK")).count() == 0)
	}
}
