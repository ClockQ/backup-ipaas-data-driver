package com.pharbers.ipaas.data.driver.libs.log

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

class TestLogDriver extends FunSuite with BeforeAndAfterAll {
	var log: PhLogDriver = _

	override def beforeAll(): Unit = {
		log = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))
		require(log != null)
	}

	test("print log") {
		log.setTraceLog("traceTest")
		log.setDebugLog("debugTest")
		log.setInfoLog("infoTest")
		log.setErrorLog("errorTest")

		for (i <- Range(0, 100)){
			log.setInfoLog("test" + i)
		}
	}
}
