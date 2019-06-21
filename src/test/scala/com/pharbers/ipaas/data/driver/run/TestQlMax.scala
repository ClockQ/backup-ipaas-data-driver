/*
 * This file is part of com.pharbers.ipaas-data-driver.
 *
 * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.pharbers.ipaas.data.driver.run

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, save2Parquet}
import env.configObj.{inst, readJobConfig}
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TestQlMax extends FunSuite {
	implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
	sd.addJar("target/ipaas-data-driver-0.1.jar")
	sd.sc.setLogLevel("ERROR")

	test("clean ql 1 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/1cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/1_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/1/201809")
	}

	test("clean ql 1 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/1cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_1_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/1")
	}

	test("test ql 1 max") {
		val phJobs = inst(readJobConfig("max_config/ql/1max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "1").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 2 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/2cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/2_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/2/201809")
	}

	test("clean ql 2 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/2cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_2_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/2")
	}

	test("test ql 2 max") {
		val phJobs = inst(readJobConfig("max_config/ql/2max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "2").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 3 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/3cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/3_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/3/201809")
	}

	test("clean ql 3 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/3cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_3_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/3")
	}

	test("test ql 3 max") {
		val phJobs = inst(readJobConfig("max_config/ql/3max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "3").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 4 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/4cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/4_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/4/201809")
	}

	test("clean ql 4 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/4cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_4_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/4")
	}

	test("test ql 4 max") { // TODO 没有 4 mkt 的线下结果
		val phJobs = inst(readJobConfig("max_config/ql/4max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)
		maxTrueDF.filter(col("MKT") === "4").show(false)
		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "4").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 5 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/5cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/5_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/5/201809")
	}

	test("clean ql 5 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/5cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_5_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/5")
	}

	test("test ql 5 max") {
		val phJobs = inst(readJobConfig("max_config/ql/5max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "5").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 6 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/6cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/6_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/6/201809")
	}

	test("clean ql 6 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/6cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_6_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/6")
	}

	test("test ql 6 max") {
		val phJobs = inst(readJobConfig("max_config/ql/6max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "6").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 7 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/7cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/7_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/7/201809")
	}

	test("clean ql 7 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/7cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_7_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/7")
	}

	test("test ql 7 max") {
		val phJobs = inst(readJobConfig("max_config/ql/7max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "7").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 8 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/8cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/8_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/8/201809")
	}

	test("clean ql 8 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/8cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_8_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/8")
	}

	test("test ql 8 max") {
		val phJobs = inst(readJobConfig("max_config/ql/8max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "8").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 9 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/9cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/9_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/9/201809")
	}

	test("clean ql 9 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/9cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_9_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/9")
	}

	test("test ql 9 max") {
		val phJobs = inst(readJobConfig("max_config/ql/9max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "9").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 10 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/10cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/10_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/10/201809")
	}

	test("clean ql 10 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/10cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_10_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/10")
	}

	test("test ql 10 max") {
		val phJobs = inst(readJobConfig("max_config/ql/10max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "10").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 11 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/11cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/11_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/11/201809")
	}

	test("clean ql 11 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/11cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_11_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/11")
	}

	test("test ql 11 max") {
		val phJobs = inst(readJobConfig("max_config/ql/11max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "11").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 12 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/12cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/12_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/12/201809")
	}

	test("clean ql 12 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/12cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_12_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/12")
	}

	test("test ql 12 max") {
		val phJobs = inst(readJobConfig("max_config/ql/12max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "12").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 13 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/13cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/13_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/13/201809")
	}

	test("clean ql 13 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/13cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_13_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/13")
	}

	test("test ql 13 max") {
		val phJobs = inst(readJobConfig("max_config/ql/13max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "13").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 14 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/14cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/14_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/14/201809")
	}

	test("clean ql 14 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/14cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_14_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/14")
	}

	test("test ql 14 max") {
		val phJobs = inst(readJobConfig("max_config/ql/14max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "14").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 15 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/15cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/15_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/15/201809")
	}

	test("clean ql 15 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/15cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_15_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/15")
	}

	test("test ql 15 max") {
		val phJobs = inst(readJobConfig("max_config/ql/15max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "15").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}

	test("clean ql 16 panel") {
		val phJobs = inst(readJobConfig("max_config/ql/16cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/16_Panel 201809.csv")

		panelERD.show(false)
		panelDF.show(false)

		val panelERDCount = panelERD.count()
		val panelDFCount = panelDF.count()
		println(panelERDCount)
		println(panelDFCount)
		assert(panelERDCount == panelDFCount)

		val panelERDUnits = panelERD.agg(sum("UNITS")).first.get(0).toString.toDouble
		val panelDFUnits = panelDF.agg(sum("Units")).first.get(0).toString.toDouble
		println(panelERDUnits)
		println(panelDFUnits)
		assert(Math.abs(panelERDUnits - panelDFUnits) < (panelDFUnits * 0.01))

		val panelERDSales = panelERD.agg(sum("SALES")).first.get(0).toString.toDouble
		val panelDFSales = panelDF.agg(sum("Sales")).first.get(0).toString.toDouble
		println(panelERDSales)
		println(panelDFSales)
		assert(Math.abs(panelERDSales - panelDFSales) < (panelDFSales * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/ql/16/201809")
	}

	test("clean ql 16 universe") {
		val phJobs = inst(readJobConfig("max_config/ql/16cleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_Universe_16_20190104.csv")

		universeERD.show(false)
		universeDF.show(false)

		val universeERDCount = universeERD.count()
		val universeDFCount = universeDF.count()
		println(universeERDCount)
		println(universeDFCount)
//		assert(universeERDCount == universeDFCount) // 可能不相等，因为新旧 PHA_ID 问题

		val universeERDFACTOR = universeERD.agg(sum("FACTOR")).first.get(0).toString.toDouble
		val universeDFFACTOR = universeDF.agg(sum("FACTOR")).first.get(0).toString.toDouble
		println(universeERDFACTOR)
		println(universeDFFACTOR)
//		assert(Math.abs(universeERDFACTOR - universeDFFACTOR) < (universeDFFACTOR * 0.01))

		val universeERDWEST_MEDICINE_INCOME = universeERD.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		val universeDFWEST_MEDICINE_INCOME = universeDF.agg(sum("WEST_MEDICINE_INCOME")).first.get(0).toString.toDouble
		println(universeERDWEST_MEDICINE_INCOME)
		println(universeDFWEST_MEDICINE_INCOME)
//		assert(Math.abs(universeERDWEST_MEDICINE_INCOME - universeDFWEST_MEDICINE_INCOME) < (universeDFWEST_MEDICINE_INCOME * 0.01))

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/ql/16")
	}

	test("test ql 16 max") {
		val phJobs = inst(readJobConfig("max_config/ql/16max.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/qilu/pha_config_repository1809/Qilu_201809_Offline_MaxResult_20190107.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "16").collect().head
		val offlineUnits = offlineResult.getString(8).toDouble
		val offlineSales = offlineResult.getString(7).toDouble

		val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
		val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble

		println(maxDFUnits)
		println(offlineUnits)
		assert(Math.abs(maxDFUnits - offlineUnits) < offlineUnits * 0.01)

		println(maxDFSales)
		println(offlineSales)
		assert(Math.abs(maxDFSales - offlineSales) < offlineSales * 0.01)
	}
}
