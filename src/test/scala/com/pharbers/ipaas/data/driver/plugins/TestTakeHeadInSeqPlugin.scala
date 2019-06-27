package com.pharbers.ipaas.data.driver.plugins

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.operators.AddColumnOperator
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}

class TestTakeHeadInSeqPlugin extends FunSuite with BeforeAndAfterAll {
	implicit var sd: PhSparkDriver = _
	var testDF: DataFrame = _

	override def beforeAll(): Unit = {
		sd = PhSparkDriver("test-driver")
		val tmp = sd.ss.implicits
		import tmp._

		testDF = List(
			("name1", "prod1", "201801", List("1", "2", "3")),
			("name2", "prod2", "201801", List("1", "2", "3")),
			("name3", "prod1", "201802", List("1", "2", "3")),
			("name4", "prod2", "201802", List("1", "2", "3"))
		).toDF("NAME", "PROD", "DATE", "RESULT")

		require(sd != null)
		require(testDF != null)
	}

	test("take head in seq") {

		val plugin = TakeHeadInSeqPlugin("TakeHeadInSeqPlugin",
			PhMapArgs(Map(
				"colName" -> PhStringArgs("RESULT")
			)),
			Seq()
		)

		val operator = AddColumnOperator(
			"AddColumnOperator",
			PhMapArgs(Map(
				"inDFName" -> PhStringArgs("inDFName"),
				"newColName" -> PhStringArgs("split")
			)),
			Seq(plugin)
		)

		val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
		val df = result.toDFArgs.get

		df.show(false)
		assert(df.columns.contains("split"))
		val head = df.collect().head
		assert(head.getString(4) == "1")
	}
}
