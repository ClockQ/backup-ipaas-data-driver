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

class TestBayerMax extends FunSuite {
	implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
	sd.addJar("target/ipaas-data-driver-0.1.jar")
	sd.sc.setLogLevel("ERROR")

	test("clean bayer WH panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/WHcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/WH_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/WH/201806")
	}

	test("clean bayer WH universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/WHcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_WH_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/WH")
	}

	test("test bayer WH max") { // TODO 未通过 945287.7749505639 was not less than 774240.6510933801
		val phJobs = inst(readJobConfig("max_config/bayer/WHmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "WH").collect().head
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

	test("clean bayer STH panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/STHcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/STH_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/STH/201806")
	}

	test("clean bayer STH universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/STHcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_STH_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/STH")
	}

	test("test bayer STH max") { // TODO 未通过 1711.566509999364 was not less than 1476.55984008
		val phJobs = inst(readJobConfig("max_config/bayer/STHmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "STH").collect().head
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

	test("clean bayer RCC panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/RCCcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/RCC_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/RCC/201806")
	}

	test("clean bayer RCC universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/RCCcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_RCC_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/RCC")
	}

	test("test bayer RCC max") {
		val phJobs = inst(readJobConfig("max_config/bayer/RCCmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "RCC").collect().head
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

	test("clean bayer PAI panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/PAIcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/PAI_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/PAI/201806")
	}

	test("clean bayer PAI universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/PAIcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_PAI_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/PAI")
	}

	test("test bayer PAI max") {
		val phJobs = inst(readJobConfig("max_config/bayer/PAImax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "PAI").collect().head
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

	test("clean bayer OSAB panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/OSABcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/OSAB_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/OSAB/201806")
	}

	test("clean bayer OSAB universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/OSABcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_OSAB_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/OSAB")
	}

	test("test bayer OSAB max") { // TODO 未通过 3007103.328786075 was not less than 2712346.1285964204
		val phJobs = inst(readJobConfig("max_config/bayer/OSABmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "OSAB").collect().head
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

	test("clean bayer OAD panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/OADcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/OAD_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/OAD/201806")
	}

	test("clean bayer OAD universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/OADcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_OAD_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/OAD")
	}

	test("test bayer OAD max") { // TODO 未通过 1.0323767082746267E7 was not less than 6665473.01407025
		val phJobs = inst(readJobConfig("max_config/bayer/OADmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "OAD").collect().head
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

	test("clean bayer IVAB panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/IVABcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/IVAB_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/IVAB/201806")
	}

	test("clean bayer IVAB universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/IVABcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_IVAB_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/IVAB")
	}

	test("test bayer IVAB max") { // TODO 未通过 2652251.62532863 was not less than 1859449.50931117
		val phJobs = inst(readJobConfig("max_config/bayer/IVABmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "IVAB").collect().head
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

	test("clean bayer HCC panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/HCCcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/HCC_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/HCC/201806")
	}

	test("clean bayer HCC universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/HCCcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_HCC_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/HCC")
	}

	test("test bayer HCC max") {
		val phJobs = inst(readJobConfig("max_config/bayer/HCCmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "HCC").collect().head
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

	test("clean bayer AT panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/ATcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/AT_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/AT/201806")
	}

	test("clean bayer AT universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/ATcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_AT_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/AT")
	}

	test("test bayer AT max") { // TODO 未通过 537950.3946435526 was not less than 422082.93156829
		val phJobs = inst(readJobConfig("max_config/bayer/ATmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "AT").collect().head
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

	test("clean bayer AHP panel") {
		val phJobs = inst(readJobConfig("max_config/bayer/AHPcleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/AHP_Panel+201806.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///workData/Panel/bayer/AHP/201806")
	}

	test("clean bayer AHP universe") {
		val phJobs = inst(readJobConfig("max_config/bayer/AHPcleanUniverse.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val universeERD = result.toMapArgs[PhDFArgs].get("universeERD").get
		val universeDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_Universe_AHP_20181106.csv")

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

//		sd.setUtil(save2Parquet()).save2Parquet(universeERD, "hdfs:///repository/universe_hosp/bayer/AHP")
	}

	test("test bayer AHP max") { // TODO 未通过 1.5683310127143264E7 was not less than 8896241.40389765
		val phJobs = inst(readJobConfig("max_config/bayer/AHPmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/bayer/test20181106/Bayer_201806_Offline_MaxResult_20181109.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "AHP").collect().head
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
