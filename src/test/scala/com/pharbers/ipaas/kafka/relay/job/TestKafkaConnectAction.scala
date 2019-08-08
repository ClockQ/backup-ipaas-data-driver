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

package com.pharbers.ipaas.kafka.relay.job

import org.scalatest.FunSuite
import env.configObj.{inst, readJobConfig}
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet
import org.apache.spark.sql.Column

class TestKafkaConnectAction extends FunSuite {
	test("kafka connect action") {
		val phJobs = inst(readJobConfig("pharbers_config/channel/kafkaconnect.yaml"))
		phJobs.head.perform(PhMapArgs(Map(
			"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
		)))
	}

	test("kafka connect source and sink test") {
		val phJobs = inst(readJobConfig("pharbers_config/channel/kafkaconnect.yaml"))
		val sparkDriver = PhSparkDriver("cui-test")
		phJobs.foreach(x =>
			x.perform(PhMapArgs(Map(
				"sparkDriver" -> PhSparkDriverArgs(sparkDriver),
				"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
			)))
		)
	}

	test("kafka connect HDFS to ES") {
		val phJobs = inst(readJobConfig("pharbers_config/channel/hdfsSinkToES.yaml"))
		phJobs.foreach(x =>
			x.perform(PhMapArgs(Map(
				"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
			)))
		)
	}

	test("kafka connect all Test") {
		val phJobs = inst(readJobConfig("pharbers_config/channel/allChanel.yaml"))
		val sparkDriver = PhSparkDriver("cui-test")
		phJobs.foreach(x =>
			x.perform(PhMapArgs(Map(
				"sparkDriver" -> PhSparkDriverArgs(sparkDriver),
				"logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
			)))
		)
	}

	test("check") {
		val path = "hdfs:///logs/testLogs/topics/source_e7871c264a0346dca6ce613e8ab0c7a9/partition=0"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		import org.apache.spark.sql.functions.expr
		val df = sd.setUtil(readParquet()).readParquet(path)
		df.groupBy("department").agg(expr("count(department) as count")).show(false)
		(1 until 1000).foreach(x => df.show(false))
	}

	test("check2") {
		val path = "hdfs:///logs/testLogs/topics/source_e7871c264a0346dca6ce613e8ab0c7a9/partition=0"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test2")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		import org.apache.spark.sql.functions.expr
		val df = sd.setUtil(readParquet()).readParquet(path)
		import org.apache.spark.sql.functions._
//		df.groupBy("department").agg(expr("count(department) as count")).show(false)
		val df1 = df.groupBy("department").agg(expr("count(department) as countResult"))
		val result = df1.selectExpr("sum(countResult) as summ", "max(countResult) as maxx", "min(countResult) as minn")
		result.show(false)
		(1 until 1000).foreach(x => df.show(false))
	}

	test("result") {
		val path = "/test/TMTest/output/e3cb67bbbe934516ba0ee033ce4b82e7"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		val df = sd.setUtil(readParquet()).readParquet(path)
		df.show(false)
	}
}
