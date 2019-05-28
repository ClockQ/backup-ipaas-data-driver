package com.pharbers.ipaas.data.driver.operators

import com.pharbers.data.util._
import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._


class TestRankOperator extends FunSuite{
    test("rank operator output must have rank column"){
        import sparkDriver.ss.implicits._

        val orderColumn = PhStringArgs("VALUE")
        val partitionColumns = PhListArgs(List(PhStringArgs("PROD"), PhStringArgs("DATE")))
        val rankColumnName = PhStringArgs("VALUE_RANK")
        val df: DataFrame = List(
            (
                List("name1", "name2", "name3", "name4"),
                List("prod1", "prod1", "prod2", "prod2"),
                List("201801", "201801", "201801", "201801"),
                List(1,2,3,4)
            )
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        val checkDf: DataFrame = List(
            (
                    List("name1", "name2", "name3", "name4"),
                    List("prod1", "prod1", "prod2", "prod2"),
                    List("201801", "201801", "201801", "201801"),
                    List(1,2,3,4),
                    List(1,2,1,2)
            )
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE_RANK")
        val result: DataFrame = CalcRank().perform(PhMapArgs(
            Map(
                "orderColumn" -> orderColumn,
                "partitionColumns" -> partitionColumns,
                "rankColumnName" -> rankColumnName,
                "data" -> PhDFArgs(df)
            )
        )).asInstanceOf[PhDFArgs].get
        assert(result.columns.contains(rankColumnName.get))
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("CHECK_VALUE_RANK") =!= col("VALUE_RANK")).count() == 0)
    }
}
