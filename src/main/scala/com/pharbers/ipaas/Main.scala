package com.pharbers.ipaas

import java.io.{File, FileInputStream}
import com.pharbers.ipaas.data.driver.api.factory.{PhFactoryTrait, getMethodMirror}
import com.pharbers.ipaas.data.driver.api.model.Job
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhJobTrait, PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.input.{InputTrait, JsonInput, YamlInput}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.save2Parquet

object Main {
	def main(args: Array[String]): Unit = {
		implicit val sparkDriver = PhSparkDriver("max-test-driver-cui")
		sparkDriver.sc.setLogLevel("ERROR")

		val logDriver = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))

		implicit val ctx = PhMapArgs(Map(
			"sparkDriver" -> PhSparkDriverArgs(sparkDriver),
			"logDriver" -> PhLogDriverArgs(logDriver)
		))

		val configReaderMap: Map[String, InputTrait] = Map(
			"json" -> JsonInput(),
			"yaml" -> YamlInput()
		)

		def inst(jobs: Seq[Job]): Seq[PhJobTrait] = {
			jobs.map(x => getMethodMirror(x.getFactory)(x, ctx).asInstanceOf[PhFactoryTrait[PhJobTrait]].inst())
		}

		def readJobConfig(path: String): Seq[Job] = {
			configReaderMap.getOrElse(path.split('.').last, throw new Exception("不能解析的文件类型")).readObjects[Job](new FileInputStream(new File(path)))
		}

		val phJobs = inst(readJobConfig("MZclean.yaml"))
		val result = phJobs.head.perform(PhMapArgs(Map()))
		val cleanDF = result.toMapArgs[PhDFArgs].get("cleanResult").get
		sparkDriver.setUtil(save2Parquet()).save2Parquet(cleanDF, "/test/testCui/maxCleanTest")
	}
}
