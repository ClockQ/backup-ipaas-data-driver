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

class TestClean extends FunSuite {
	implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
	sd.addJar("target/ipaas-data-driver-0.1.jar")
	sd.sc.setLogLevel("ERROR")

	test("clean panel") {
		val phJobs = inst(readJobConfig("max_config/common/cleanPanel.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get
		val panelDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/xlt/XLT_Panel 201806.csv")

		panelERD.show(false)
		panelDF.show(false)

		println(panelERD.count())
		println(panelDF.count())

		println(panelERD.agg(sum("UNITS")).first.get(0))
		println(panelDF.agg(sum("Units")).first.get(0))

		println(panelERD.agg(sum("SALES")).first.get(0))
		println(panelDF.agg(sum("Sales")).first.get(0))
	}
}
