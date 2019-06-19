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

import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import env.configObj.{inst, readJobConfig}
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet

class TestXltMax extends FunSuite {
	implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
	sd.addJar("target/ipaas-data-driver-0.1.jar")
	sd.sc.setLogLevel("ERROR")

	test("test xlt max") {
		val phJobs = inst(readJobConfig("max_config/nhwa/clean.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sd),
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))

		val cleanDF = result.toMapArgs[PhDFArgs].get("cleanResult").get
		val cleanTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc")

		cleanDF.show(false)
		cleanTrueDF.show(false)

		println(cleanDF.count())
		println(cleanTrueDF.count())

		println(cleanDF.agg(sum("UNITS")).first.get(0))
		println(cleanTrueDF.agg(sum("UNITS")).first.get(0))

		println(cleanDF.agg(sum("SALES")).first.get(0))
		println(cleanTrueDF.agg(sum("SALES")).first.get(0))
	}
}
