package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.data.util.spark.sparkDriver
import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class testWindow extends FunSuite {
    import sparkDriver.ss.implicits._
    val partitionColumnNames = List("PROD")
    val dateColName = "DATE"
    val valueColumnName = "VALUE"
    val outputColumnName = "RESULT"

    test("year growth by window"){

        val df: DataFrame = List(
            ("name1", "prod1", "201701", 1),
            ("name2", "prod2", "201701", 2),
            ("name3", "prod1", "201801", 2),
            ("name4", "prod2", "201801", 3)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201701", 1, 0.0),
            ("name2", "prod2", "201701", 2, 0.0),
            ("name3", "prod1", "201801", 2, 1.0),
            ("name4", "prod2", "201801", 3, 0.5)
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")
        val yearGrowthPlugin = CalcYearGrowth(PhMapArgs(Map(
            "dateColName" -> PhStringArgs(dateColName),
            "valueColumnName" -> PhStringArgs(valueColumnName),
            "partitionColumnNames" -> PhListArgs(partitionColumnNames.map(x => PhStringArgs(x)))
        ))).perform(PhNoneArgs).asInstanceOf[PhColArgs].get

        val result = df.withColumn(outputColumnName, yearGrowthPlugin)
        result.show()
        assert(result.columns.contains(outputColumnName))
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }

    test("ring growth"){

        val df: DataFrame = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod2", "201801", 2),
            ("name3", "prod1", "201802", 2),
            ("name4", "prod2", "201802", 3)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201801", 1, 0.0),
            ("name2", "prod2", "201801", 2, 0.0),
            ("name3", "prod1", "201802", 2, 1.0),
            ("name4", "prod2", "201802", 3, 0.5)
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

        df.withColumn("test", when(expr("VALUE > '1'"), 0)).show()
        val growthPlugin = CalcRingGrowth(PhMapArgs(Map(
            "dateColName" -> PhStringArgs(dateColName),
            "valueColumnName" -> PhStringArgs(valueColumnName),
            "partitionColumnNames" -> PhListArgs(partitionColumnNames.map(x => PhStringArgs(x)))
        ))).perform(PhNoneArgs).asInstanceOf[PhColArgs].get

        val result = df.withColumn(outputColumnName, growthPlugin)
        result.show()
        assert(result.columns.contains(outputColumnName))
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }
}
