package com.pharbers.ipaas.data.driver.libs.log

import com.pharbers.util.log.PhLogable
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.logging.log4j.{LogManager, Logger}

class TestLogDriver extends FunSuite with BeforeAndAfterAll {
	var log: PhLogDriver = _

	override def beforeAll(): Unit = {
//		sd = PhSparkDriver("test-driver")
		implicit val logger: Logger = LogManager.getLogger(this)
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

	test("log able"){
		val fun = PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobID")).get().get
		println(fun(List("1", "2", "3")))
	}

}