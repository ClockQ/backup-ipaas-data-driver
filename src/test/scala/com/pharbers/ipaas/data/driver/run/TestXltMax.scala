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
//package com.pharbers.ipaas.data.driver.run
//
//import org.scalatest.FunSuite
//import org.apache.spark.sql.functions._
//import env.configObj.{inst, readJobConfig}
//import com.pharbers.ipaas.data.driver.api.work._
//import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
//import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
//import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet}
//
//class TestXltMax extends FunSuite {
//	implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
//	sd.addJar("target/ipaas-data-driver-0.1.jar")
//	sd.sc.setLogLevel("ERROR")
//
//	test("clean xlt XLT panel") {
//		val phJobs = inst(readJobConfig("max_config/xlt/XLTcleanPanel.yaml"))
//		val result = phJobs.head.perform(PhMapArgs(Map(
//			"sparkDriver" -> PhSparkDriverArgs(sd),
//			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
//		)))
//
//		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
//		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/xlt/XLT_Panel 201806.csv")
//
//		panelERD.show(false)
//		panelDF.show(false)
//
//		val panelERDCount = panelERD.count()
//		val panelDFCount = panelDF.count()
//		println(panelERDCount)
//		println(panelDFCount)
//		assert(panelERDCount == panelDFCount)
//
//		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
//		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
//		println(panelERDUnits)
//		println(panelDFUnits)
//		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))
//
//		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
//		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
//		println(panelERDSales)
//		println(panelDFSales)
//		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))
//
////		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/xlt/xlt/201806")
//	}
//
//	test("clean xlt XLT universe") {
//		val phJobs = inst(readJobConfig("max_config/xlt/XLTcleanUniverse.yaml"))
//		val result = phJobs.head.perform(PhMapArgs(Map(
//			"sparkDriver" -> PhSparkDriverArgs(sd),
//			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
//		)))
//
//		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
//		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/xlt/XLT_Universe_XLT_20181115.csv")
//
//		universeERD.show(false)
//		universeDF.show(false)
//
//		val universeERDCount = universeERD.count()
//		val universeDFCount = universeDF.count()
//		println(universeERDCount)
//		println(universeDFCount)
////		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题
//
//		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
//		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
//		println(universeERDFACTOR)
//		println(universeDFFACTOR)
////		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))
//
//		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
//		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
//		println(universeERDWEST_MEDICINE_INCOME)
//		println(universeDFWEST_MEDICINE_INCOME)
////		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))
//
////		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/xlt/xlt")
//	}
//
//	test("test xlt XLT max") {
//		val phJobs = inst(readJobConfig("max_config/xlt/XLTmax.yaml"))
//		val result = phJobs.head.perform(PhMapArgs(Map(
//			"sparkDriver" -> PhSparkDriverArgs(sd),
//			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
//		)))
//
//		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
//		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/xlt/XLT_201806_Offline_MaxResult_20181115.csv")
//
//		maxDF.show(false)
//		maxTrueDF.show(false)
//
//		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL").collect().head
//		val offlineUnits = offlineResult.getString(8).toDouble
//		val offlineSales = offlineResult.getString(7).toDouble
//
//		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
//		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble
//
//		println(maxDFUnits)
//		println(offlineUnits)
//		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)
//
//		println(maxDFSales)
//		println(offlineSales)
//		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
//	}
//}
