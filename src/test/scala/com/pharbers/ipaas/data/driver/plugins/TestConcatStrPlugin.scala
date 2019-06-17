package com.pharbers.ipaas.data.driver.plugins

import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.operators.AddColumnOperator
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhOperatorTrait2, PhPluginTrait2, PhStringArgs}

class TestConcatStrPlugin extends FunSuite with BeforeAndAfterAll {
    implicit var sd: PhSparkDriver = _
    var operator: PhOperatorTrait2[DataFrame] = _
    var plugin: PhPluginTrait2[Column] = _
    var testDF: DataFrame = _
    val dilimiter: String = ","

    override def beforeAll(): Unit = {
        sd = PhSparkDriver("test-driver")
        val tmp = sd.ss.implicits
        import tmp._

        testDF = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod1", "201801", 2),
            ("name3", "prod2", "201801", 3),
            ("name4", "prod2", "201801", 4)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        plugin = ConcatStrPlugin(
            "ConcatStrPlugin",
            PhMapArgs(Map(
                "columns" -> PhStringArgs("NAME#PROD"),
                "dilimiter" -> PhStringArgs(dilimiter)
            )),
            Seq.empty
        )

        operator = AddColumnOperator(
            "AddColumnOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "newColName" -> PhStringArgs("newColName")
            )),
            Seq(plugin)
        )

        require(operator != null)
        require(sd != null)
        require(testDF != null)
    }

    test("add column by concat") {
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        val df = result.toDFArgs.get
        val tmp = df.take(1).head
        val name = tmp.getString(0)
        val prod = tmp.getString(1)
        val newColName = tmp.getString(4)

        assert(result.toDFArgs.get.columns.contains("newColName"))
        assert(name + dilimiter + prod == newColName)
    }
}
