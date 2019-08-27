package com.pharbers.ipaas.data.driver.plugins

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.api.work._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.operators.AddColumnOperator

class TestDropLastInSeqPlugin extends FunSuite with BeforeAndAfterAll {

    import env.sparkObj._
    import sparkDriver.ss.implicits._

    var testDF: DataFrame = _

    override def beforeAll(): Unit = {
        testDF = List(
            ("name1", "prod1", "201801", List("1", "2", "3")),
            ("name2", "prod2", "201801", List("1", "2", "3")),
            ("name3", "prod1", "201802", List("1", "2", "3")),
            ("name4", "prod2", "201802", List("1", "2", "3"))
        ).toDF("NAME", "PROD", "DATE", "RESULT")

        require(testDF != null)
    }

    test("drop last in seq and mkString by space") {

        val plugin = DropLastInSeqPlugin("DropLastInSeqPlugin",
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
        assert(head.getString(4) == "1 2")
    }
}
