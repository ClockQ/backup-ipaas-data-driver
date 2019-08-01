package com.pharbers.ipaas.kafka.relay.job

import com.pharbers.ipaas.data.driver.api.work.{PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import env.configObj.{inst, readJobConfig}
import org.scalatest.FunSuite

class pressureTest extends FunSuite {
	test("kafka connect all Test") {
		val sparkDriver = PhSparkDriver("cui-test")
		val log = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))
		def testExe(n: Int): Unit ={
			println(s"第${n}次测试开始===========")
			val phJobs = inst(readJobConfig("pharbers_config/channel/pressureTest.yaml"))
			phJobs.foreach(x =>
				x.perform(PhMapArgs(Map(
					"sparkDriver" -> PhSparkDriverArgs(sparkDriver),
					"logDriver" -> PhLogDriverArgs(log)
				)))
			)
			println(s"第${n}次测试结束==========")
		}
		(1 to 100).foreach(n => testExe(n))
//		(1 to 1).foreach(n => testExe(n))
	}
}
