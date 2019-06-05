package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhListArgs, PhMapArgs, PhNoneArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class TestConcatStrPlugin extends FunSuite{
    implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")
    import sparkDriver.ss.implicits._

    val df: DataFrame = List(
        ("name1", "prod1", "201801", 1, 2),
        ("name2", "prod2", "201801", 2, 3),
        ("name3", "prod1", "201802", 2, 4),
        ("name4", "prod2", "201802", 3, 5)
    ).toDF("NAME", "PROD", "DATE", "VALUE", "VALUE2")
    test("base concat str plugin"){

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201801", 1, 2, "name1+prod1+1"),
            ("name2", "prod2", "201801", 2, 3, "name2+prod2+2"),
            ("name3", "prod1", "201802", 2, 4, "name3+prod1+2"),
            ("name4", "prod2", "201802", 3, 5, "name4+prod2+3")
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE2", "CHECK_RESULT")

        val result = ConcatStrPlugin("", PhMapArgs(Map(
            "columnList" -> PhListArgs(List(PhStringArgs("NAME"),PhStringArgs("PROD"),PhStringArgs("VALUE"))),
            "dilimiter" -> PhStringArgs("+")
        ))).perform(PhNoneArgs).toDFArgs.get
        result.show(false)
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }
}
