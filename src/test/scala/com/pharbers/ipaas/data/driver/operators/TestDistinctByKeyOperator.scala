package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestDistinctByKeyOperator extends FunSuite with BeforeAndAfterAll {

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

    test("distinct by prod, keep max VALUE") {
        val operator = DistinctByKeyOperator(
            "DistinctByKeyOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "keys" -> PhStringArgs("PROD"),
                "chooseBy" -> PhStringArgs("VALUE"),
                "chooseFun" -> PhStringArgs("max")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        val df = result.toDFArgs.get
        assert(df.count() == 2)
        assert(df.take(2).head.getInt(3) == 2)
        assert(df.take(2).last.getInt(3) == 4)
    }

    test("distinct by prod, keep min VALUE") {
        val operator = DistinctByKeyOperator(
            "DistinctByKeyOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName"),
                "keys" -> PhStringArgs("PROD"),
                "chooseBy" -> PhStringArgs("VALUE"),
                "chooseFun" -> PhStringArgs("min")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        val df = result.toDFArgs.get
        assert(df.count() == 2)
        assert(df.take(2).head.getInt(3) == 1)
        assert(df.take(2).last.getInt(3) == 3)
    }
}
