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
import com.pharbers.ipaas.data.driver.libs.spark.util.readCsv
import env.configObj.{inst, readJobConfig}
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TestTqMax extends FunSuite {
	implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
	sd.addJar("target/ipaas-data-driver-0.1.jar")
	sd.sc.setLogLevel("ERROR")

	test("test tq RP max") { //TODO 未通过
		val phJobs = inst(readJobConfig("max_config/tq/RPmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/TQ/TQ_201806_Offline_MaxResult_20181126.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "RP").collect().head
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

	test("test tq SA max") {  //TODO 未通过
		val phJobs = inst(readJobConfig("max_config/tq/SAmax.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
		val maxTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/TQ/TQ_201806_Offline_MaxResult_20181126.csv")

		maxDF.show(false)
		maxTrueDF.show(false)

		val offlineResult = maxTrueDF.filter(col("CATEGORY") === "ALL" && col("MKT") === "SA").collect().head
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
