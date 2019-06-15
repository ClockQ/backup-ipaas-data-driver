package com.pharbers.ipaas.data.driver.run

import env.configObj._
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet

class TestNhwaMax extends FunSuite {
	test("test nhwa clean") {
		implicit val sd: PhSparkDriver = env.sparkObj()

		val phJobs = inst(readJobConfig("max_config/nhwa/clean.yaml"))
		val result = phJobs.head.perform()

		val cleanDF = result.toMapArgs[PhDFArgs].get("readCpaFile").get
		cleanDF.show(false)
//
//		val cleanTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc")
//
//		cleanDF.show(false)
//		cleanTrueDF.show(false)
//
//		println(cleanDF.count())
//		println(cleanTrueDF.count())
//
//		println(cleanDF.agg(sum("UNITS")).first.get(0))
//		println(cleanDF.agg(sum("SALES")).first.get(0))
//
//		println(cleanTrueDF.agg(sum("UNITS")).first.get(0))
//		println(cleanTrueDF.agg(sum("SALES")).first.get(0))
	}

//	test("test nhwa panel") {
//		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//		val jobs = configObj.readJobConfig("pharbers_config/clean.yaml")
//		val phJobs = jobs.map(x => getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//		val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//		val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get
//		val cleanTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc")
//
//		cleanDF.show(false)
//		cleanTrueDF.show(false)
//
//		println(cleanDF.count())
//		println(cleanTrueDF.count())
//
//		println(cleanDF.agg(sum("UNITS")).first.get(0))
//		println(cleanDF.agg(sum("SALES")).first.get(0))
//
//		println(cleanTrueDF.agg(sum("UNITS")).first.get(0))
//		println(cleanTrueDF.agg(sum("SALES")).first.get(0))
//	}
//
//	test("test nhwa max") {
//		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//
//		val jobs = configObj.readJobConfig("pharbers_config/max.yaml")
//		val phJobs = jobs.map(x => getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
//		val result = phJobs.head.perform(PhMapArgs(Map.empty))
//
//		val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
//		val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/qi/qi/new_max_true")
//
//		println(maxDF.count())
//		println(maxTrueDF.count())
//
//		println(maxDF.agg(sum("f_units")).first.get(0))
//		println(maxDF.agg(sum("f_sales")).first.get(0))
//
//		println(maxTrueDF.agg(sum("f_units")).first.get(0))
//		println(maxTrueDF.agg(sum("f_sales")).first.get(0))
//	}
}
