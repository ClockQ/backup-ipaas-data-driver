package com.pharbers.ipaas.data.driver.run

import env.configObj._
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet

class TestNhwaMax extends FunSuite {
    implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
    sd.addJar("target/ipaas-data-driver-0.1.jar")
    sd.sc.setLogLevel("ERROR")

    test("test nhwa clean") {
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

    test("test nhwa panel") {
        val phJobs = inst(readJobConfig("max_config/common/panelByCpa.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map(
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
        )))

        val panelDF = result.toMapArgs[PhDFArgs].get("reducedCpa").get
//		val panelTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/qi/qi/1809_panel.csv")

        panelDF.show(false)
//		panelTrueDF.show(false)

        println(panelDF.count())
//		println(panelTrueDF.count())

        println(panelDF.agg(sum("UNITS")).first.get(0))
//		println(panelTrueDF.agg(sum("Units")).first.get(0))

        println(panelDF.agg(sum("SALES")).first.get(0))
//		println(panelTrueDF.agg(sum("Sales")).first.get(0))
    }

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
