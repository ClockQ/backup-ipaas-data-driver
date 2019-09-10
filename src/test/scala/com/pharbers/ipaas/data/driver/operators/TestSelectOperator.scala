package com.pharbers.ipaas.data.driver.operators

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhOperatorTrait, PhStringArgs}

class TestSelectOperator extends FunSuite with BeforeAndAfterAll {

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

    test("select column") {
        val operator = SelectOperator(
            "SelectOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "selects" -> PhStringArgs("NAME#PROD")
            )),
            Seq()
        )

        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        val df = result.toDFArgs.get
        val columns = df.columns
        assert(columns.length == 2)
        assert(columns.contains("NAME"))
        assert(columns.contains("PROD"))
    }
}
