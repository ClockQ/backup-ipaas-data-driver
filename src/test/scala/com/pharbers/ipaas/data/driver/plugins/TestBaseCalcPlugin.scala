//package com.pharbers.ipaas.data.driver.plugin
//
//import com.pharbers.ipaas.data.driver.api.work._
//import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//import org.scalatest.FunSuite
//
//class TestBaseCalcPlugin extends FunSuite {
//    implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")
//    import sparkDriver.ss.implicits._
//
//    val df: DataFrame = List(
//        ("name1", "prod1", "201801", 1, 2),
//        ("name2", "prod2", "201801", 2, 3),
//        ("name3", "prod1", "201802", 2, 4),
//        ("name4", "prod2", "201802", 3, 5)
//    ).toDF("NAME", "PROD", "DATE", "VALUE", "VALUE2")
//    test("base calc plugin"){
//
//        val checkDf: DataFrame = List(
//            ("name1", "prod1", "201801", 1, 2, -0.5),
//            ("name2", "prod2", "201801", 2, 3, 0.5),
//            ("name3", "prod1", "201802", 2, 4, 1.0),
//            ("name4", "prod2", "201802", 3, 5, 3.5)
//        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_VALUE2", "CHECK_RESULT")
//
//        val col = ExprPlugin().perform(PhMapArgs(Map(
//            "exprString" -> PhStringArgs("((VALUE * VALUE2) - (VALUE + VALUE2)) / 2")
//        ))).toColArgs.get
//        val result = df.withColumn("RESULT", col)
//        result.show(false)
//        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
//    }
//
//}
