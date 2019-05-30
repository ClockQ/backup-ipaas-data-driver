package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.data.util.spark.sparkDriver
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.plugins._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class TestSortPlugin extends FunSuite{
    import sparkDriver.ss.implicits._
    sparkDriver.sc.addJar("D:\\code\\pharbers\\ipaas-data-driver\\target\\ipaas-data-driver-0.1.jar")
    val df: DataFrame = List(
        ("name2", 2, 3),
        ("name3", 2, 4),
        ("name1", 1, 2),
        ("name4", 3, 5)
    ).toDF("NAME", "VALUE", "VALUE2")
    test("sort plugin"){

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201801", 1, 2, 1),
            ("name2", "prod2", "201801", 2, 3, 2),
            ("name3", "prod1", "201802", 2, 4, 3),
            ("name4", "prod2", "201802", 3, 5, 4)
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE2", "CHECK_RESULT")

        val result = SortPlugin().perform(PhMapArgs(Map(
            "sortList" -> PhListArgs(List(PhStringArgs("VALUE"),PhStringArgs("VALUE2"))),
            "orderStr" -> PhStringArgs("asc"),
            "df" -> PhDFArgs(df)
        ))).toDFArgs.get
//        result.show(false)

        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }
}
