package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.operators.AddColumnOperator
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestGenerateObjectIdPlugin extends FunSuite with BeforeAndAfterAll {

    import env.sparkObj._
    import sparkDriver.ss.implicits._

    var testDF: DataFrame = _

    override def beforeAll(): Unit = {
        testDF = List(
            ("name1", "prod1", "2018", "1"),
            ("name2", "prod1", "2018", "2"),
            ("name3", "prod2", "2018", "3"),
            ("name4", "prod2", "2018", "4")
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        require(testDF != null)
    }

    test("generate object id") {
        val plugin = GenerateObjectIdPlugin(
            "GenerateObjectIdPlugin",
            PhMapArgs(Map()),
            Seq.empty
        )

        val operator = AddColumnOperator(
            "AddColumnOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "newColName" -> PhStringArgs("_id")
            )),
            Seq(plugin)
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        result.toDFArgs.get.show(false)
        assert(result.toDFArgs.get.columns.contains("_id"))
    }
}
