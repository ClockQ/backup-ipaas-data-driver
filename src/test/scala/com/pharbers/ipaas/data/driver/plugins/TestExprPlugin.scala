package com.pharbers.ipaas.data.driver.plugins

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.operators.AddColumnOperator

class TestExprPlugin extends FunSuite with BeforeAndAfterAll {

    import env.sparkObj._
    import sparkDriver.ss.implicits._

    var testDF: DataFrame = _

    override def beforeAll(): Unit = {
        testDF = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod1", "201801", 2),
            ("name3", "prod2", "201801", 3),
            ("name4", "prod2", "201801", 4)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        require(testDF != null)
    }

    test("add column by expr, cast old column as double") {
        val plugin = ExprPlugin(
            "ExprPlugin",
            PhMapArgs(Map(
                "exprString" -> PhStringArgs("cast(VALUE as double)")
            )),
            Seq.empty
        )(ctx)

        val operator = AddColumnOperator(
            "AddColumnOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "newColName" -> PhStringArgs("newColName")
            )),
            Seq(plugin)
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))

        val df = result.toDFArgs.get
        df.show(false)
        assert(df.columns.contains("newColName"))
    }

    test("add column by expr, add lit value") {
        val plugin = ExprPlugin(
            "ExprPlugin",
            PhMapArgs(Map(
                "exprString" -> PhStringArgs("'a'")
            )),
            Seq.empty
        )

        val operator = AddColumnOperator(
            "AddColumnOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "newColName" -> PhStringArgs("newColName")
            )),
            Seq(plugin)
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))

        val df = result.toDFArgs.get
        df.show(false)
        assert(df.columns.contains("newColName"))
    }
}
