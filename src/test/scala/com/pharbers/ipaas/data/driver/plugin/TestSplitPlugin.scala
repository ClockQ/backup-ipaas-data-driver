package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TestSplitPlugin extends FunSuite {
    implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")
    import sparkDriver.ss.implicits._

    val df: DataFrame = List(
        ("name1", "prod1", "201801", List("1","2","3")),
        ("name2", "prod2", "201801", List("1","2","3")),
        ("name3", "prod1", "201802", List("1","2","3")),
        ("name4", "prod2", "201802", List("1","2","3"))
    ).toDF("NAME", "PROD", "DATE", "RESULT")
    test("seq[string] drop last and mkString"){

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201801", 1, 2,"1 2"),
            ("name2", "prod2", "201801", 2, 3, "1 2"),
            ("name3", "prod1", "201802", 2, 4, "1 2"),
            ("name4", "prod2", "201802", 3, 5, "1 2")
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE2", "CHECK_RESULT")

        val result = SplitPlugin().perform(PhMapArgs(Map(
            "splitedColName" -> PhStringArgs("RESULT"),
            "df" -> PhDFArgs(df)
        ))).toDFArgs.get.cache()
        result.show(false)
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }
}
