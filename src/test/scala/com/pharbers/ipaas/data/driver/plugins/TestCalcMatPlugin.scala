package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhStringArgs}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TestCalcMatPlugin extends FunSuite {

    import env.sparkObj._
    import sparkDriver.ss.implicits._

    val partitionColumnNames = List("PROD")
    val dateColName = "DATE"
    val valueColumnName = "VALUE"
    val outputColumnName = "RESULT"

    test("MAT plugin") {
        val df: DataFrame = List(
            ("name1", "prod1", "201701", 1),
            ("name2", "prod1", "201702", 1),
            ("name3", "prod1", "201801", 1),
            ("name4", "prod1", "201802", 1)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201701", 1, 1),
            ("name2", "prod1", "201702", 1, 2),
            ("name3", "prod1", "201801", 1, 2),
            ("name4", "prod1", "201802", 1, 2)
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

        val growthPlugin = CalcMatPlugin("CalcMatPluginTest",
            PhMapArgs(Map(
                "valueColumnName" -> PhStringArgs(valueColumnName),
                "dateColName" -> PhStringArgs(dateColName),
                "partitionColumnNames" -> PhStringArgs("PROD")
            )),
            Seq()
        ).perform(PhMapArgs(Map().empty)).asInstanceOf[PhColArgs].get

        val result = df.withColumn(outputColumnName, growthPlugin)
        result.show()
        assert(result.columns.contains(outputColumnName))
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME") && col("CHECK_PROD") === col("PROD"))
                .filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }
}
