package com.pharbers.ipaas.data.driver.tm.tmConfig

import com.pharbers.ipaas.data.driver.api.work.{PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet, save2Parquet}
import env.configObj.{inst, readJobConfig}
import org.scalatest.FunSuite

class readParquetTest extends FunSuite {

	test("readParquetTest") {
		val sparkDriver = PhSparkDriver("cui-test")
		val log = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))

		def testExe(n: Int): Unit = {
			println(s"第${n}次测试开始===========")
			val phJobs = inst(readJobConfig("pharbers_config/json/tmTestConfig/test.json"))
			phJobs.foreach(x =>
				x.perform(PhMapArgs(Map(
					"sparkDriver" -> PhSparkDriverArgs(sparkDriver),
					"logDriver" -> PhLogDriverArgs(log)
				)))
			)
			println(s"第${n}次测试结束==========")
		}

		(1 to 1).foreach(n => testExe(n))
	}

	test("randTest") {
		val sparkDriver = PhSparkDriver("cui-test")
		val log = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))

		def testExe(n: Int): Unit = {
			println(s"第${n}次测试开始===========")
			val phJobs = inst(readJobConfig("pharbers_config/json/tmTestConfig/rand.json"))
			phJobs.foreach(x =>
				x.perform(PhMapArgs(Map(
					"sparkDriver" -> PhSparkDriverArgs(sparkDriver),
					"logDriver" -> PhLogDriverArgs(log)
				)))
			)
			println(s"第${n}次测试结束==========")
		}

		(1 to 1).foreach(n => testExe(n))
	}

	test("export max") {
		implicit val sparkDriver: PhSparkDriver = PhSparkDriver("cui-test")
		val path = "/workData/Max/"
		val savePath = "/test/maxDownload/pfizer/pfizer201905/"
		val lst = List("6d375790-bdbd-48b8-ab37-5b2a08a6b443",
			"813fc27e-713a-4ffe-bd16-2731f9475911",
			"d7073a52-a61b-4956-ab03-a8011379f1ad",
			"6fe0b606-0c2e-4d1c-8a34-4f58e03923c4",
			"f614b4fc-198d-460c-81fb-d8993aa9f1cf",
			"bf984979-1802-4b05-b432-c0776b860368",
			"bdd32631-7928-4c16-aa5f-4719a6dd3cbc",
			"e272b6d5-ccc9-4130-b986-1b97f2d0f8b0",
			"50b37386-a2e6-4ff6-b0b4-55d0028aab03",
			"dcbfeae4-2ec1-4f65-b9d0-0e74b7af5ec9",
			"e88178b8-0f5f-4583-8526-c97cb4367b36",
			"c1c0e5f3-ba31-4cbf-9837-a3bd9a6bcb3d",
			"aba0934d-0566-4773-ad58-4f7990b7ee80",
			"f77d3666-45ac-4810-9dc2-523a9e6d3d57")
		lst.foreach(x => {
			val df = sparkDriver.setUtil(readParquet()).readParquet(path + x)
			val market = df.select("market").take(1).head.get(0).toString

			df.coalesce(1).write
				.format("csv")
				.option("encoding", "UTF-8")
				.option("header", value = true)
				.option("delimiter", "#")
				.save(savePath + market)
			println(market + "完成")
		})

	}

	test("read tm input") {
		implicit val sparkDriver: PhSparkDriver = PhSparkDriver("cui-test")
		val path = "/test/TMTest/input/"
		val fileList = List("cal_data.csv",
			"curves.csv",
			"manager.csv",
			"weightages.csv"
		)
		fileList.foreach(fileName => {
			val df = sparkDriver.setUtil(readCsv()).readCsv(path + fileName)
			df.show(false)
			sparkDriver.setUtil(save2Parquet()).save2Parquet(df, "/test/TMTest/inputParquet/" + fileName.split("\\.").head)
		}
		)
	}
}
