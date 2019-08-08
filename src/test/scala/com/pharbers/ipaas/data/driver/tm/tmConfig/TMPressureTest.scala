package com.pharbers.ipaas.data.driver.tm.tmConfig

import com.pharbers.ipaas.data.driver.api.work.{PhNoneArgs, PhStringArgs}
import com.pharbers.ipaas.kafka.relay.http.PhChanelHttpRequest
import org.scalatest.FunSuite

import scala.util.parsing.json.JSON

class TMPressureTest extends FunSuite{
	test("TMPressureTest") {
		println("压力测试开始==========")
		def exec(): Unit ={
			val postData =
				s"""
				   |{
				   |    "id": "d9a8c6e546a04de5ad2a4246b26db51f",
				   |    "config": {
				   |        "name": "a4d2498d67e5479a9dfa213cce8a1382",
				   |        "topic": "test001",
				   |        "bucketName": "pharbers-resources",
				   |        "ossKey": "TMSalesTest.json"
				   |    },
				   |    "status": "creating"
				   |}
             """.stripMargin
			val url = "http://192.168.100.195:8080/spark/job/run"
			val response = PhChanelHttpRequest(url, postData).getResponseAsStr
			if (response.code > 400) {
				val body = JSON.parseFull(response.body).get.asInstanceOf[Map[String, Any]]
				val errMsg = body("message").toString
				throw new Exception(errMsg)
			}
			Thread.sleep(10000)
		}
		(1 to 100).foreach(x => {
			println("第" + x + "次测试开始===========")
			exec()
			println("第" + x + "次测试结束===========")
		}
		)
	}
}
