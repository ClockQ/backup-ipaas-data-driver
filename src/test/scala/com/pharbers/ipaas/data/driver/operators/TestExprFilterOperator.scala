package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.plugins.ExprPlugin
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestExprFilterOperator extends FunSuite with BeforeAndAfterAll {

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

    test("filter dataframe by expr") {
        val operator = ExprFilterOperator(
            "ExprFilterOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "filter" -> PhStringArgs("DATE == 201801")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        assert(result.get.count() == 2)
    }
}
