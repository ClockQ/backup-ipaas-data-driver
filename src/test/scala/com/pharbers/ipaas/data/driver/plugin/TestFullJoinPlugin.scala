package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.data.util.spark.sparkDriver
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.plugins._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TestFullJoinPlugin extends FunSuite{
    import sparkDriver.ss.implicits._
    val leftDf: DataFrame = List(
        ("name1"),
        ("name2")
    ).toDF("LEFT")

    val rightDf: DataFrame =List(
        ("name5"),
        ("name6")
    ).toDF("RIGHT")
    test("base concat str plugin"){

        val checkDf: DataFrame = List(
            ("name1", "name5"),
            ("name1", "name6"),
            ("name2", "name5"),
            ("name2", "name6")
        ).toDF("CHECK_LEFT", "CHECK_RIGHT")

        val result = FullJoinPlugin().perform(PhMapArgs(Map(
            "leftDF" -> PhDFArgs(leftDf),
            "rightDF" -> PhDFArgs(rightDf)
        ))).toDFArgs.get
        result.show(false)
        assert(result.join(checkDf, col("CHECK_LEFT") === col("LEFT") && col("CHECK_RIGHT") === col("RIGHT")).count() == 4)
    }
}
