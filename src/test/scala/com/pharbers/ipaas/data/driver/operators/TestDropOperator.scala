package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestDropOperator extends FunSuite with BeforeAndAfterAll {

    import env.sparkObj._
    import sparkDriver.ss.implicits._

    var testDF: DataFrame = _

    override def beforeAll(): Unit = {
        testDF = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod1", "201801", 2),
            ("name3", "prod2", "201802", 3),
            ("name4", "prod2", "201802", 4)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        require(testDF != null)
    }

    test("delete one column from dataframe") {
        val operator = DropOperator(
            "DropOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "drops" -> PhStringArgs("VALUE")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        assert(result.get.columns.length == 3)
    }

    test("delete multi column from dataframe") {
        val operator = DropOperator(
            "DropOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "drops" -> PhStringArgs("DATE#VALUE")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        assert(result.get.columns.length == 2)
    }
}
