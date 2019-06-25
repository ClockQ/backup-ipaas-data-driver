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
//import com.pharbers.ipaas.data.driver.libs.spark.util.{readParquet, save2Parquet}
//import org.apache.spark.sql.SaveMode
//import org.apache.spark.sql.functions._
//import org.scalatest.FunSuite
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author EDZ
//  * @version 0.0
//  * @since 2019/06/20 17:27
//  * @note 一些值得注意的地方
//  */
//class TestAstellas extends FunSuite {
//    test("test astellas gycx clean") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/astellas/cleanGycx.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get
//        cleanDF.show(false)
//        println(cleanDF.count())
//
//        sd.setUtil(save2Parquet()).save2Parquet(cleanDF, "hdfs:///test/dcs/Clean/gycx/astellas", SaveMode.Overwrite)
//    }
//
//    test("test astellas cpa clean") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/astellas/cleanCpa.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get
//
//        cleanDF.show(false)
//        println(cleanDF.count())
//
//        sd.setUtil(save2Parquet()).save2Parquet(cleanDF, "hdfs:///test/dcs/Clean/cpa/astellas", SaveMode.Overwrite)
//
//        //        println(cleanDF.agg(sum("UNITS")).first.get(0))
//        //        println(cleanDF.agg(sum("SALES")).first.get(0))
//    }
//
//    test("test astellas sample cpa hosp") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/astellas/sampleCpaHosp.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val sampleHosp = result.toMapArgs[PhDFArgs].get("sample_hosp").get
//
//        sd.setUtil(save2Parquet()).save2Parquet(sampleHosp, "hdfs:///test/dcs/Clean/sampleHosp/astellas/ALKP_cpa", SaveMode.Overwrite)
//        println(sampleHosp.count())
//        sampleHosp.show(false)
//
//    }
//
//    test("test astellas sample gycx hosp") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/astellas/sampleGycxHosp.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val sampleHosp = result.toMapArgs[PhDFArgs].get("sample_hosp").get
//
//        sd.setUtil(save2Parquet()).save2Parquet(sampleHosp, "hdfs:///test/dcs/Clean/sampleHosp/astellas/ALKP_gycx", SaveMode.Overwrite)
//        println(sampleHosp.count())
//        sampleHosp.show(false)
//
//    }
//
//    test("test astellas three cpa hosp") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/astellas/cleanCpaSourceTable.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val resultDF = result.toMapArgs[PhDFArgs].get("result").get
//
//        sd.setUtil(save2Parquet()).save2Parquet(resultDF, "hdfs:///test/dcs/Clean/treeSource/astellas/cpa")
//        println(resultDF.count())
//        resultDF.show(false)
//
//    }
//
//    test("test astellas three gycx hosp") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/astellas/cleanGycxSourceTable.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val resultDF = result.toMapArgs[PhDFArgs].get("result").get
//
//        sd.setUtil(save2Parquet()).save2Parquet(resultDF, "hdfs:///test/dcs/Clean/treeSource/astellas/gycx")
//        println(resultDF.count())
//
//    }
//
//    test("test astellas panel") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//        val jobs = Config.readJobConfig("pharbers_config/astellas/panel.yaml")
//        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//        val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//        val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
//
//        sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///test/dcs/Clean/panel/astellas/ALKP", SaveMode.Overwrite)
//
//        val panelTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Panel/5eec5688-f3e6-4249-a468-c4276e79cf2e")
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
//    test("check panel") {
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//        sd.sc.setLogLevel("error")
//        val prodErd = {
//            sd.setUtil(readParquet())
//                    .readParquet("hdfs:///repository/prod_etc_dis_max/astellas").filter(col("MARKET") === "Allelock")
//                    .withColumn("min2", trim(regexp_replace(concat(col("ETC_PRODUCT_NAME"), col("ETC_DOSAGE_NAME"), col("ETC_PACKAGE_DES"), col("ETC_PACKAGE_NUMBER"), col("ETC_CORP_NAME")), " ", "")))
//        }
//        val hospErd = {
//            sd.setUtil(readParquet())
//                    .readParquet("hdfs:///repository/hosp_dis_max")
//                    .filter("PHA_IS_REPEAT == 0")
//                    .dropDuplicates(List("PHA_HOSP_ID"))
//        }
//
//
//        val panelErd = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/Clean/panel/astellas/ALKP")
//
//        val panelTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Panel/5eec5688-f3e6-4249-a468-c4276e79cf2e")
//
//        val all = panelErd.selectExpr("HOSPITAL_ID as hosp", "SALES as VALUE", "PRODUCT_ID")
//                .join(hospErd, col("hosp") === col("HOSPITAL_ID"))
//                .join(prodErd.select("min2", "_id"), col("PRODUCT_ID") === col("_id"))
//
//
////        all.filter("PHA_HOSP_ID == 'PHA0013881'").show(false)
//
//        val a = panelErd.selectExpr("HOSPITAL_ID as hosp", "SALES as VALUE", "PRODUCT_ID")
//                .join(hospErd, col("hosp") === col("HOSPITAL_ID"))
//                .join(prodErd.select("min2", "_id"), col("PRODUCT_ID") === col("_id"))
//                .select("VALUE","min2","PHA_HOSP_ID")
////                .join(panelTrueDF, col("PHA_HOSP_ID") === col("HOSP_ID") && col("min2") === regexp_replace(col("Prod_Name"), "\\|", ""))
//                .join(panelTrueDF, col("PHA_HOSP_ID") === col("HOSP_ID") && col("min2") === regexp_replace(col("Prod_Name"), "\\|", ""), "left")
////                .drop("PHA_HOSP_ID")
////                .distinct()
//
////        a.withColumn("count", lit(0)).groupBy(a.columns.head, a.columns.tail:_*).agg(count("count")).filter("count(count) > 1").show(false)
//        a.filter("HOSP_ID is null").show(false)
////        a.filter("HOSP_ID is null").select("PHA_HOSP_ID").distinct().count()
//        val res = a.withColumn("check", expr("abs(SALES - VALUE)")).filter("check > 0.01")
//        println(panelErd.count())
//        println(a.count())
//        println(res.count())
//        res.show(false)
//
//
//    }
//}
