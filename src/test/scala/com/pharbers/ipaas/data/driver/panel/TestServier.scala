///*
// * This file is part of com.pharbers.ipaas-data-driver.
// *
// * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
// */
//
//package com.pharbers.ipaas.data.driver.panel
//
//import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs}
//import com.pharbers.ipaas.data.driver.config.Config
//import com.pharbers.ipaas.data.driver.factory.{PhFactory, PhJobFactory}
//import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
//import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet, save2Parquet}
//import org.apache.spark.sql.SaveMode
//import org.apache.spark.sql.functions.sum
//import org.scalatest.FunSuite
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author EDZ
//  * @version 0.0
//  * @since 2019/06/21 16:34
//  * @note 一些值得注意的地方
//  */
//class TestServier extends FunSuite {
//    test("test servier panel") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/service/cleanPanel.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val panelERD = result.toMapArgs[PhDFArgs].get("panelData").get
//
//        sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///test/dcs/Clean/panel/servier/IHD", SaveMode.Overwrite)
//
//        val panelTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/servier/201806_all_correct/IHD_Panel 201806.csv")
//
//        println(panelERD.select("PRODUCT_ID").distinct().count())
//        println(panelTrueDF.select("Prod_Name").distinct().count())
//
//        println(panelERD.select("HOSPITAL_ID").distinct().count())
//        println(panelTrueDF.select("HOSP_ID").distinct().count())
//
//        println(panelERD.count())
//        println(panelTrueDF.count())
//
//        println(panelERD.agg(sum("UNITS")).first.get(0))
//        println(panelERD.agg(sum("SALES")).first.get(0))
//
//        println(panelTrueDF.agg(sum("UNITS")).first.get(0))
//        println(panelTrueDF.agg(sum("SALES")).first.get(0))
//
//        //        panelERD.show(false)
//        //        val hospDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/hosp_dis_max").selectExpr("HOSPITAL_ID as HOSPITAL_ID_M", "PHA_HOSP_ID")
//        //        val cpaDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/pha")
//        //        val a = panelERD.join(hospDF, col("HOSPITAL_ID") === col("HOSPITAL_ID_M"))
//        //                .selectExpr("PHA_HOSP_ID as PHA_HOSP_ID_M")
//        //                .join(panelTrueDF, col("PHA_HOSP_ID_M") === col("HOSP_ID"))
//        //        println(a.count())
//    }
//
//    test("test servier Universe") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/service/cleanUniverse.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val panelERD = result.toMapArgs[PhDFArgs].get("universeClean").get
//
//        sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///test/dcs/Clean/universe/servier/IHD", SaveMode.Overwrite)
//
//        val panelTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/servier/201806_all_correct/Servier_Universe_IHD_20181119.csv")
//
//
//        println(panelERD.count())
//        println(panelTrueDF.count())
//
//
//        //        panelERD.show(false)
//        //        val hospDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/hosp_dis_max").selectExpr("HOSPITAL_ID as HOSPITAL_ID_M", "PHA_HOSP_ID")
//        //        val cpaDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/pha")
//        //        val a = panelERD.join(hospDF, col("HOSPITAL_ID") === col("HOSPITAL_ID_M"))
//        //                .selectExpr("PHA_HOSP_ID as PHA_HOSP_ID_M")
//        //                .join(panelTrueDF, col("PHA_HOSP_ID_M") === col("HOSP_ID"))
//        //        println(a.count())
//    }
//}
