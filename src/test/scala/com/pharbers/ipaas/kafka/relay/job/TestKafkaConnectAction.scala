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
		val path = "/test/TMTest/input/TMInput0813/"
		val fileNameList = List("curves.csv")
		//		val fileNameList = List("cal_data.csv", "competition.csv", "curves.csv", "level_data.csv", "manager.csv", "p_action_kpi.csv",
		//			"standard_time.csv", "weightages.csv")
		val savePath = "/test/TMTest/inputParquet/TMInputParquet0813/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readCsv()).readCsv(path + name)
			sd.setUtil(save2Parquet()).save2Parquet(df, savePath + name.split("\\.").head)
			println(name + "保存完毕")
		})
	}

	test("UCB input") {
		//		val path = "/test/UCBTest/input/"
		val path = "/test/UCBTest/input/input0814/"
		val fileNameList = List("cal_data.csv", "competitor.csv", "curves.csv", "weightages.csv", "p_data1.csv",
			"p_data2.csv")
		val savePath = "/test/UCBTest/inputParquet/input0814/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readCsv()).readCsv(path + name)
			df.show(false)
			sd.setUtil(save2Parquet()).save2Parquet(df, savePath + name.split("\\.").head)
			println(name + "保存完毕")
		})
	}

	test("UCB output") {
		val path = "/test/UCBTest/output/"
		val fileNameList = List("CityReport", "FinalReport", "HospitalReport", "NextBudget", "ProductReport",
			"RepresentativeReport", "SummaryReport")
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			df.show(false)
			println(name + "展示完毕")
		})
	}

	test("TM new output") {
		val path = "/test/TMTest/output/"
		val fileNameList = List("Assessment", "TMCompetitor", "TMResult")
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			df.show(false)
			println(name + "展示完毕")
		})
	}

	test("TM new output1") {
		val path = "/test/TMTest/output/"
		val savePath = "/test/TMTest/output/csvResult/"
		val fileNameList = List("Assessment", "TMCompetitor", "TMResult")
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			println(name + "开始保存")
			sd.setUtil(save2Csv()).save2Csv(df, savePath + name)
			println(name + "保存完成")
		})
	}

	test("UCB output 0814") {
		val path = "/test/UCBTest/output/output0814/"
		val fileNameList = List("CompetitorReport", "FinalSummary", "HospitalReport", "UCBResult")
		val savePath = "/test/UCBTest/output/output0814Csv/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			//			df.show(false)
			sd.setUtil(save2Csv()).save2Csv(df, savePath + name)
			println(name + "展示完毕")
		})
	}

	test("UCB check output") {
		val path = "/test/UCBTest/output/"
		val fileNameList = List("UCBcheck")
		val savePath = "/test/UCBTest/output/UCBcheckCsv/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			//			df.show(false)
			sd.setUtil(save2Csv()).save2Csv(df, savePath + name)
			println(name + "展示完毕")
		})
	}

	test("TM input 0815") {
		val path = "/test/TMTest/input/TMInput0815/"
		val fileNameList = List("cal_data.csv", "competitor.csv", "curves.csv", "level_data.csv", "manager.csv",
			"standard_time.csv", "weightages.csv")
		val savePath = "/test/TMTest/inputParquet/TMInputParquet0815/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			val df = sd.setUtil(readCsv()).readCsv(path + name)
			println(name + "保存完成")
			sd.setUtil(save2Parquet()).save2Parquet(df, savePath + name.split("\\.").head)
			println(name + "保存完毕")
		})
	}

	test("TM output 0815 show") {
		val path = "/test/TMTest/output/TMoutput0815/"
		val fileNameList = List("Assessment", "TMCompetitor", "TMResult")
		//		val savePath = "/test/UCBTest/output/UCBcheckCsv/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			df.show(false)
			//			sd.setUtil(save2Csv()).save2Csv(df, savePath + name)
			println(name + "展示完毕")
		})
	}

	test("UCB output 0816") {
		val path = "/test/UCBTest/output/output0816/"
		val fileNameList = List("CompetitorReport", "HospitalReport", "UCBResult")
		val savePath = "/test/UCBTest/output/output0816/UCBCsv/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			println(name + "开始保存完毕")
			sd.setUtil(save2Csv()).save2Csv(df, savePath + name)
			println(name + "保存完毕")
		})
	}

	test("UCB output 081601") {
		val path = "/test/testCui/kafkaTest003/"
		val fileNameList = List("bd3e19a8112749c09514bfb11cfcbb21", "bbb2c6df4c3b4a2c8464236b93a5367b",
			"822090a0880d42e2b91cbe399acbd04c")
		val savePath = "/test/UCBTest/output/output0816/UCBCsv081601/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			println(name + "开始保存完毕")
			sd.setUtil(save2Csv()).save2Csv(df, savePath + name)
			println(name + "保存完毕")
		})
	}

	test("UCB output 081701") {
		val path = "/test/UCBTest/output/output0817/"
		val fileNameList = List("CompetitorReport", "FinalSummary",
			"HospitalReport", "UCBResult")
		//		val savePath = "/test/UCBTest/output/output0816/UCBCsv081601/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			//			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			println(name + "开始展示")
			df.show(false)
			//			sd.setUtil(save2Csv()).save2Csv(df, savePath + name)
			println(name + "展示完毕")
		})
	}

	test("UCB input 0820") {
		val path = "/test/UCBTest/input/Input0820/"
		val fileNameList = List("cal_data.csv", "competitor.csv", "curves.csv", "p_data1.csv", "p_data2.csv",
			"p_data3.csv", "p_data4.csv", "p_data5.csv", "p_data6.csv", "weightages.csv")
		val savePath = "/test/UCBTest/inputParquet/TMInputParquet0820/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			val df = sd.setUtil(readCsv()).readCsv(path + name)
			println(name + "保存开始")
			sd.setUtil(save2Parquet()).save2Parquet(df, savePath + name.split("\\.").head)
			println(name + "保存完毕")
		})
	}

	test("Download QL Max Result") {
		val path = "/workData/Max/"
		val fileNameList = List(
			"94432b98-7f55-5a0d-6107-788a34b43130",
			"739a3c8c-0343-43bf-cbfc-e7d6a976233e",
			"be981d52-28a6-5922-306b-f1494203751c",
			"d3cf6ffe-22ad-258a-b755-3695e2ec8c5d",
			"ed9cf686-2d4f-0549-fe0a-f52e85ef64ac",
			"c6cd3821-660c-1720-9486-d6b1e7daa469",
			"751f253e-e879-a13f-70ea-6ce079c741b6",
			"c1494ddd-6d0f-9939-9a93-cf0253941c64",
			"ba41a3a1-1e53-6773-e7a7-e26082560375",
			"1065e795-209c-b79b-7fc4-9237d567a02a",
			"6d733b50-2754-bdf1-fa88-df1371f8cb48",
			"869c76f7-fab5-57a7-51db-e3d26568f71d",
			"e0b1ee8b-91c4-3d3a-bbc3-704ded1a960b",
			"432c0526-aee8-3ed2-e632-6df41880346d",
			"13273aa8-3330-b86b-8677-f1d2e77e9f83",
			"b8f72ed2-9818-82fe-ed7c-9952e952f860"
		)
		val savePath = "/test/maxDownload/QILU201906/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			val df = sd.setUtil(readCsv()).readCsv(path + name, 31.toChar.toString)
			val market = df.select("market").take(1).head.get(0).toString
			println(market + "保存开始")
			sd.setUtil(save2Csv()).save2Csv(df, savePath + market)
			println(market + "保存完毕")
		})
	}

	test("Download Pfizer Max Result") {
		val path = "/workData/Max/"
		val fileNameList = List(
			"a7cf6e01-b906-49e5-8c0e-871042a8400a",
			"a185b5f5-6281-403f-8da1-5bc179cd5e3c",
			"39871f45-1dae-46dc-a2fe-6a071ef8679e",
			"faa7cb02-6e30-45d5-a1cc-4c9c3ca05248",
			"5fa9cf28-312b-4502-85d5-6bc058bd0c7c",
			"a1be86a5-f2f4-4279-92db-9879210da7af",
			"df8f4f0f-f684-4f0a-ba58-900bc3bd9e90",
			"4634bc21-f20d-437f-8cbc-6af47635c237",
			"9bbbac2e-a046-42fb-8faf-1479634ad4bc",
			"80207a88-e345-4e28-8431-661331a087e0",
			"f8c86278-6540-4804-894f-9ddb563853f8",
			"d4e15522-c77b-4ea4-b272-e0d0fbd410e7",
			"6f5f5f46-ad2b-4e76-9f52-0e68e4238f18",
			"bbbd841c-cb9b-47a6-bbc6-a3fe7b83ef77",
			"195c4263-745e-46dd-a078-7180f4a3ddee",
			"5ed33408-aeb4-4e66-bbd5-b9477d0b6d68",
			"97544325-6f3c-4843-8cab-06956e027829",
			"f28a3dae-4c9d-482f-a225-646a9cf7535a",
			"9fb1b52a-20a0-461d-9486-697e2b48a6bf",
			"f1efc5e2-1764-4013-9b4f-e2bfa2d9bc13",
			"165a9339-6e4c-4ec5-beab-6713da87b492",
			"598676a3-2280-4e9a-a392-50f7e4f7a404",
			"9d021ff6-bcad-4024-b075-4b97daf76244"
		)
		val savePath = "/test/maxDownload/Pfizer201906/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		fileNameList.foreach(name => {
			val df = sd.setUtil(readParquet()).readParquet(path + name)
			val market = df.select("market").take(1).head.get(0).toString
			println(market + "保存开始")
			sd.setUtil(save2Csv()).save2Csv(df, savePath + market)
			println(market + "保存完毕")
		})
	}

	test("spark ID Test") {
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		println("appId" + sd.sc.getConf.getAppId)
		println("applicationName" + sd.applicationName)
		println("完成")
	}

	test("curves_n input 0820") {
		val path = "/test/TMTest/input/TMInput0815/"
		val fileNameList = List("curves-n.csv")
		val savePath = "/test/TMTest/inputParquet/TMInputParquet0815/"
		implicit val sd: PhSparkDriver = PhSparkDriver("cui-test")
		import com.pharbers.ipaas.data.driver.libs.spark.util._
		//	option("inferSchema", true)
		fileNameList.foreach(name => {
			val df = sd.setUtil(readCsv()).readCsv(path + name)
			println(name + "保存开始")
			sd.setUtil(save2Parquet()).save2Parquet(df, savePath + name.split("\\.").head)
			println(name + "保存完毕")
		})
	}
}
