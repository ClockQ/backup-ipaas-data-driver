package com.pharbers.ipaas.data.driver.run

import java.util.UUID

import env.configObj.{inst, readJobConfig}
import test.tag.MaxTag
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.log.{PhLogFormat, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet, save2Parquet}
import env.sparkObj

@MaxTag
class TestNhwaMax extends FunSuite {


    implicit val sd: PhSparkDriver = sparkObj.ctx.getAs[PhSparkDriverArgs]("sparkDriver").get.get
    sd.sc.setLogLevel("ERROR")

    test("test nhwa MZ clean") {
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZclean.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map(
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
        )))

        val cleanDF = result.toMapArgs[PhDFArgs].get("cleanResult").get
        val cleanTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc")

        cleanDF.show(false)
        cleanTrueDF.show(false)

        val cleanDFCount = cleanDF.count()
        val cleanTrueDFCount = cleanTrueDF.count()
        println(cleanDFCount)
        println(cleanTrueDFCount)
        assert(cleanDFCount == cleanTrueDFCount)

        val cleanDFUnits = cleanDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        val cleanTrueDFUnits = cleanTrueDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        println(cleanDFUnits)
        println(cleanTrueDFUnits)
        assert(cleanDFUnits == cleanTrueDFUnits)

        val cleanDFSales = cleanDF.agg(sum("SALES")).first.get(0).toString.toDouble
        val cleanTrueDFSales = cleanTrueDF.agg(sum("SALES")).first.get(0).toString.toDouble
        println(cleanDFSales)
        println(cleanTrueDFSales)
        assert(cleanDFSales == cleanTrueDFSales)

    }

    test("test nhwa MZ panel") {
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZpanel.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map(
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
        )))

        val panelDF = result.toMapArgs[PhDFArgs].get("panelResult").get
        val panelTrueDF = sd.setUtil(readCsv()).readCsv("hdfs:///test/qi/qi/1809_panel.csv")
//
//        panelDF.show(false)
//        panelTrueDF.show(false)
//
        val panelDFCount = panelDF.count()
        val panelTrueDFCount = panelTrueDF.count()
        println(panelDFCount)
        println(panelTrueDFCount)
//
//        val panelDFUnits = panelDF.agg(sum("UNITS")).first.get(0).toString.toDouble
//        val panelTrueDFUnits = panelTrueDF.agg(sum("Units")).first.get(0).toString.toDouble
//        println(panelDFUnits)
//        println(panelTrueDFUnits)
//        assert(Math.abs(panelDFUnits - panelTrueDFUnits) < panelTrueDFUnits * 0.01)
//
//        val panelDFSales = panelDF.agg(sum("SALES")).first.get(0).toString.toDouble
//        val panelTrueDFSales = panelTrueDF.agg(sum("Sales")).first.get(0).toString.toDouble
//        println(panelDFSales)
//        println(panelTrueDFSales)
//        assert(Math.abs(panelDFSales - panelTrueDFSales) < panelTrueDFSales * 0.01)
    }

	test("test nhwa MZ max") {
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZmax.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map(
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logFormat" -> PhLogFormat(formatMsg("test_ nhwa MZ max user", "test_ nhwa MZ max traceID", "test_ nhwa MZ max jobId")).get()
        )))

        val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
        val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/qi/qi/new_max_true")

        maxDF.show(false)
        maxTrueDF.show(false)

        val maxDFCount = maxDF.count()
        val maxTrueDFCount = maxTrueDF.count()
        println(maxDFCount)
        println(maxTrueDFCount)
        assert(maxDFCount == maxTrueDFCount)

        val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
        val maxTrueDFUnits = maxTrueDF.agg(sum("f_units")).first.get(0).toString.toDouble
        println(maxDFUnits)
        println(maxTrueDFUnits)
        assert(Math.abs(maxDFUnits - maxTrueDFUnits) < maxTrueDFUnits * 0.01)

        val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble
        val maxTrueDFSales = maxTrueDF.agg(sum("f_sales")).first.get(0).toString.toDouble
        println(maxDFSales)
        println(maxTrueDFSales)
        assert(Math.abs(maxDFSales - maxTrueDFSales) < maxTrueDFSales * 0.01)
    }

    test("nhwa all"){
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/temp.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map(
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logFormat" -> PhLogFormat(formatMsg("test_ nhwa MZ max user", "test_ nhwa MZ max traceID", "test_ nhwa MZ max jobId")).get()
        )))
    }

    test("test submit result"){
        sd.sc.setLogLevel("ERROR")

        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZmax.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map()))
        val maxTrueDF = result.toMapArgs[PhDFArgs].get("maxResult").get.cache()
        val maxDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/test0912").cache()
        val maxDFCount = maxDF.count()
        val maxTrueDFCount = maxTrueDF.count()
        println(maxDFCount)
        println(maxTrueDFCount)
//        assert(maxDFCount == maxTrueDFCount)

        val maxDFUnits = maxDF.agg(sum("f_units")).first.get(0).toString.toDouble
        val maxTrueDFUnits = maxTrueDF.agg(sum("f_units")).first.get(0).toString.toDouble
        println(maxDFUnits)
        println(maxTrueDFUnits)
//        assert(Math.abs(maxDFUnits - maxTrueDFUnits) < maxTrueDFUnits * 0.01)

        val maxDFSales = maxDF.agg(sum("f_sales")).first.get(0).toString.toDouble
        val maxTrueDFSales = maxTrueDF.agg(sum("f_sales")).first.get(0).toString.toDouble
        println(maxDFSales)
        println(maxTrueDFSales)
//        assert(Math.abs(maxDFSales - maxTrueDFSales) < maxTrueDFSales * 0.01)
    }

    test("submit panel"){
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZpanelByCpa.yaml"))

        val result = phJobs.head.perform(PhMapArgs(Map()))

        val panelTrueDF = result.toMapArgs[PhDFArgs].get("panelResult").get.cache()
        val panelDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Panel/test0912").cache()

        val panelDFCount = panelDF.count()
        val panelTrueDFCount = panelTrueDF.count()
        println(panelDFCount)
        println(panelTrueDFCount)
//
//        val panelDFUnits = panelDF.agg(sum("UNITS")).first.get(0).toString.toDouble
//        val panelTrueDFUnits = panelTrueDF.agg(sum("Units")).first.get(0).toString.toDouble
//        println(panelDFUnits)
//        println(panelTrueDFUnits)
//        //        assert(Math.abs(panelDFUnits - panelTrueDFUnits) < panelTrueDFUnits * 0.01)
//
//        val panelDFSales = panelDF.agg(sum("SALES")).first.get(0).toString.toDouble
//        val panelTrueDFSales = panelTrueDF.agg(sum("Sales")).first.get(0).toString.toDouble
//        println(panelDFSales)
//        println(panelTrueDFSales)
        //        assert(Math.abs(panelDFSales - panelTrueDFSales) < panelTrueDFSales * 0.01)
    }

    test("submit clean"){
//        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZclean.yaml"))
//        val result = phJobs.head.perform(PhMapArgs(Map()))

        val cleanTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc").filter("YM == 201809")
                .selectExpr("HOSPITAL_ID as h", "PRODUCT_ID as p","SALES as s", "UNITS as u")
                .withColumn("_id", lit(UUID.randomUUID().toString))
                .cache()
        val cleanDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/cpa-test0912").filter("YM == 201809").cache()

        println(cleanDF.select("PRODUCT_ID").distinct().count())
        println(cleanTrueDF.select("p").distinct().count())


        val joinRes = cleanTrueDF.join(cleanDF, expr("h == HOSPITAL_ID and p == PRODUCT_ID"), "left").cache()
        println(joinRes.count())
        println(joinRes.filter("HOSPITAL_ID is null").count())
        joinRes.filter("HOSPITAL_ID is null").show(100, truncate = false)
//
////        cleanDF.show(false)
////        cleanTrueDF.show(false)
//
//        val cleanDFCount = cleanDF.count()
//        val cleanTrueDFCount = cleanTrueDF.count()
//        println(cleanDFCount)
//        println(cleanTrueDFCount)

//        val cleanDFUnits = cleanDF.agg(sum("UNITS")).first.get(0).toString.toDouble
//        val cleanTrueDFUnits = cleanTrueDF.agg(sum("UNITS")).first.get(0).toString.toDouble
//        println(cleanDFUnits)
//        println(cleanTrueDFUnits)
//
//        val cleanDFSales = cleanDF.agg(sum("SALES")).first.get(0).toString.toDouble
//        val cleanTrueDFSales = cleanTrueDF.agg(sum("SALES")).first.get(0).toString.toDouble
//        println(cleanDFSales)
//        println(cleanTrueDFSales)
    }
}
