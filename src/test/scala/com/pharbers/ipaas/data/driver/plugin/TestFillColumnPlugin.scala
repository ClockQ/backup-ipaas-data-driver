package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.plugins._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class TestFillColumnPlugin extends FunSuite {
    implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")
    import sparkDriver.ss.implicits._

    val df: DataFrame = List(
        ("name1", "prod1", "201801", 1, null),
        ("name2", "prod2", "201801", 2, null),
        ("name3", "prod1", "201802", 2, null),
        ("name4", "prod2", "201802", 3, null)
    ).toDF("NAME", "PROD", "DATE", "VALUE", "VALUE2")
    test("add column by one column and default value"){

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201801", 1, "DEFAULT"),
            ("name2", "prod2", "201801", 2, "DEFAULT"),
            ("name3", "prod1", "201802", 2, "DEFAULT"),
            ("name4", "prod2", "201802", 3, "DEFAULT")
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

        val result = FillColumnPlugin().perform(PhMapArgs(Map(
            "defaultValue" -> PhStringArgs("DEFAULT"),
            "columnNameNew" -> PhStringArgs("RESULT"),
            "columnNameOld" -> PhStringArgs("VALUE2"),
            "replaceColumnName" -> PhStringArgs("NAME"),
            "df" -> PhDFArgs(df)
        ))).toDFArgs.get
        result.show(false)
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }
}
