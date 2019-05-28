//package com.pharbers.ipaas.data.driver.operators
//
//import org.apache.spark.sql.DataFrame
//
///**
//  * @description:
//  * @author: clock
//  * @date: 2019-05-23 17:47
//  */
//object testCalcYTD extends App {
//
//    import com.pharbers.data.util._
//    import com.pharbers.data.util.spark._
//    import sparkDriver.ss.implicits._
//    import org.apache.spark.sql.functions._
//
//    lazy val company_id: String = "5ca069bceeefcc012918ec72"
//    lazy val missHospERD = Parquet2DF("/repository/miss_hosp" + "/" + company_id)
//            .withColumn("sales", lit(1))
//
//
//    val b = CalcYTD().exec(missHospERD)
//    b.show(false)
//    b.printSchema()
//}
