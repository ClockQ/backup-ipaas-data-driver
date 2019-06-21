package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.Config
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet, save2Parquet}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.FunSuite

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
class TestJobFactory extends FunSuite {
    test("test nhwa max") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/max.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
        val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/qi/qi/new_max_true")

        println(maxDF.count())
        println(maxTrueDF.count())

        println(maxDF.agg(sum("f_units")).first.get(0))
        println(maxDF.agg(sum("f_sales")).first.get(0))

        println(maxTrueDF.agg(sum("f_units")).first.get(0))
        println(maxTrueDF.agg(sum("f_sales")).first.get(0))
    }

    test("test nhwa clean") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/testClean.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get
        val cleanTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc")

        cleanDF.show(false)
        cleanTrueDF.show(false)

        println(cleanDF.count())
        println(cleanTrueDF.count())

        println(cleanDF.agg(sum("UNITS")).first.get(0))
        println(cleanDF.agg(sum("SALES")).first.get(0))

        println(cleanTrueDF.agg(sum("UNITS")).first.get(0))
        println(cleanTrueDF.agg(sum("SALES")).first.get(0))
    }

    test("test nhwa miss hosp") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/nhwa/missHosp.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val missHosp = result.toMapArgs[PhDFArgs].get("not_arrival_hosp").get
        val trueDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/miss_hosp/5ca069bceeefcc012918ec72")

        println(missHosp.count())
        println(trueDF.count())

        missHosp.show(false)

    }

    test("test nhwa sample hosp") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/nhwa/sampleCpaHosp.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val sampleHosp = result.toMapArgs[PhDFArgs].get("sample_hosp").get

        val trueDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/sample_hosp/5ca069bceeefcc012918ec72/mz")

        println(sampleHosp.count())
        println(trueDF.count())
        sampleHosp.show(false)

    }

    test("test pfizer coef clean") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/cleanCoef.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val cleanDF = result.toMapArgs[PhDFArgs].get("coefMatchHosp").get

//        sd.setUtil(save2Parquet()).save2Parquet(cleanDF, "hdfs:///test/dcs/Clean/coef/pfizer")
        cleanDF.show(false)

        //        println(cleanDF.filter("YM == 201804").select("HOSPITAL_ID", "PRODUCT_ID").distinct().count())

        //        println(cleanTrueDF.agg(sum("UNITS")).first.get(0))
        //        println(cleanTrueDF.agg(sum("SALES")).first.get(0))
    }

    test("test pfizer gycx clean") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/cleanGycx.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

//        sd.setUtil(save2Parquet()).save2Parquet(cleanDF, "hdfs:///test/dcs/Clean/gycx/pfizer")
        //        cleanDF.show(false)

        //        println(cleanDF.filter("YM == 201804").select("HOSPITAL_ID", "PRODUCT_ID").distinct().count())

        //        println(cleanTrueDF.agg(sum("UNITS")).first.get(0))
        //        println(cleanTrueDF.agg(sum("SALES")).first.get(0))
    }

    test("test pfizer cpa clean") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/cleanCpa.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

//        sd.setUtil(save2Parquet()).save2Parquet(cleanDF, "hdfs:///test/dcs/Clean/cpa/pfizer")

        //        println(cleanDF.agg(sum("UNITS")).first.get(0))
        //        println(cleanDF.agg(sum("SALES")).first.get(0))
    }

    test("test pfizer full hosp clean") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/cleanFullHosp.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

//        sd.setUtil(save2Parquet()).save2Parquet(cleanDF, "hdfs:///test/dcs/Clean/fullHosp/pfizer")

        //        println(cleanDF.filter("YM == 201804").select("HOSPITAL_ID", "PRODUCT_ID").distinct().count())
        //        println(cleanDF.agg(sum("UNITS")).first.get(0))
        //        println(cleanDF.agg(sum("SALES")).first.get(0))

    }

    test("test pfizer miss hosp") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/missHosp.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val missHosp = result.toMapArgs[PhDFArgs].get("not_arrival_hosp").get

        //        sd.setUtil(save2Parquet()).save2Parquet(missHosp, "hdfs:///test/dcs/Clean/missHosp/pfizer")
        missHosp.show(false)

    }

    test("test pfizer sample cpa hosp") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/sampleCpaHosp.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val sampleHosp = result.toMapArgs[PhDFArgs].get("sample_hosp").get

//        sd.setUtil(save2Parquet()).save2Parquet(sampleHosp, "hdfs:///test/dcs/Clean/sampleHosp/pfizer/CNS_R_cpa")
        println(sampleHosp.count())

    }

    test("test pfizer sample gycx hosp") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/sampleGycxHosp.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val sampleHosp = result.toMapArgs[PhDFArgs].get("sample_hosp").get

//        sd.setUtil(save2Parquet()).save2Parquet(sampleHosp, "hdfs:///test/dcs/Clean/sampleHosp/pfizer/CNS_R_gycx")
        println(sampleHosp.count())

    }

    test("test pfizer panel") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/pfizer/pfizerPanel.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val panelERD = result.toMapArgs[PhDFArgs].get("panelERD").get

//        sd.setUtil(save2Parquet()).save2Parquet(panelERD, "hdfs:///test/dcs/Clean/panel/pfizer/CNS_R")

        val panelTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Panel/0db067e5-c3f2-4112-93be-f447f2fede74")

        println(panelERD.select("PRODUCT_ID").distinct().count())
        println(panelTrueDF.select("Prod_Name").distinct().count())

        println(panelERD.select("HOSPITAL_ID").distinct().count())
        println(panelTrueDF.select("HOSP_ID").distinct().count())

        println(panelERD.count())
        println(panelTrueDF.count())

        println(panelERD.agg(sum("UNITS")).first.get(0))
        println(panelERD.agg(sum("SALES")).first.get(0))

        println(panelTrueDF.agg(sum("UNITS")).first.get(0))
        println(panelTrueDF.agg(sum("SALES")).first.get(0))

        //        panelERD.show(false)
        //        val hospDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/hosp_dis_max").selectExpr("HOSPITAL_ID as HOSPITAL_ID_M", "PHA_HOSP_ID")
        //        val cpaDF = sd.setUtil(readParquet()).readParquet("hdfs:///repository/pha")
        //        val a = panelERD.join(hospDF, col("HOSPITAL_ID") === col("HOSPITAL_ID_M"))
        //                .selectExpr("PHA_HOSP_ID as PHA_HOSP_ID_M")
        //                .join(panelTrueDF, col("PHA_HOSP_ID_M") === col("HOSP_ID"))
        //        println(a.count())
    }

    test("check prod") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
        sd.sc.setLogLevel("ERROR")
        val prodErd = {
            sd.setUtil(readParquet())
                    .readParquet("hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73").filter(col("MARKET") === "INF")
                    .withColumn("min2", trim(regexp_replace(concat(col("ETC_PRODUCT_NAME"), col("ETC_DOSAGE_NAME"), col("ETC_PACKAGE_DES"), col("ETC_PACKAGE_NUMBER"), col("ETC_CORP_NAME")), " ", "")))
        }
        val gycxErd = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/Clean/fullHosp/inf").filter(col("MARKET").isNotNull)
        val gycx = {
            sd.setUtil(readCsv()).readCsv("hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_FullHosp.txt", 31.toChar.toString)
                    .na.fill(value = "0", cols = Array("VALUE", "STANDARD_UNIT"))
                    .withColumn("PRODUCT_NAME", when(col("PRODUCT_NAME").isNull, col("MOLE_NAME"))
                            .otherwise(col("PRODUCT_NAME")))
                    .withColumn("MONTH", col("MONTH").cast(IntegerType))
                    .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
                            .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
                    .withColumn("PRODUCT_NAME", trim(col("PRODUCT_NAME")))
                    .withColumn("DOSAGE", trim(col("DOSAGE")))
                    .withColumn("PACK_DES", trim(col("PACK_DES")))
                    .withColumn("PACK_NUMBER", trim(col("PACK_NUMBER")))
                    .withColumn("CORP_NAME", trim(col("CORP_NAME")))
                    .withColumn("min1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
                    .withColumn("ym", concat(col("YEAR"), col("MONTH")))
        }
        val prodMatch = sd.setUtil(readCsv()).readCsv("hdfs:///data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv")
                .select("MIN_PRODUCT_UNIT", "MIN_PRODUCT_UNIT_STANDARD", "MOLE_NAME")
                .distinct()
        val marketMatch = sd.setUtil(readCsv()).readCsv("hdfs:///data/pfizer/pha_config_repository1901/Pfizer_MarketMatchTable_20190422.csv")
                .filter(s"MARKET like 'INF%'")
                .withColumnRenamed("MOLE_NAME", "MOLE_NAME_M")
        val markets_product_match = prodMatch.join(marketMatch, prodMatch("MOLE_NAME") === marketMatch("MOLE_NAME_M"))
        val temp = gycx.join(markets_product_match, gycx("min1") === markets_product_match("MIN_PRODUCT_UNIT")).drop(markets_product_match("MIN_PRODUCT_UNIT"))
                .withColumn("min1", trim(regexp_replace(col("min1"), " ", "")))


        println(gycxErd.count())
        println(temp.count())
        println("hosp")
        println(gycxErd.select("HOSPITAL_ID").distinct().count())
        println(temp.select("HOSP_ID").distinct().count())

        println("prod")
        println(gycxErd.select("PRODUCT_ID").distinct().count())
        println(temp.select("min1").distinct().count())
        //        val a = List("ETC_PRODUCT_NAME","ETC_CORP_NAME","ETC_MOLE_NAME","ETC_PACKAGE_DES","ETC_PACKAGE_NUMBER","ETC_DOSAGE_NAME")
        val cpaProd = gycxErd.select("PRODUCT_ID").distinct()
                .join(prodErd, col("PRODUCT_ID") === col("ETC_PRODUCT_ID"))


        //        cpaErd.select("PRODUCT_ID").distinct()
        //                .join(prodErd, col("PRODUCT_ID") === col("ETC_PRODUCT_ID"))
        //                .join(temp.select("MIN_PRODUCT_UNIT_STANDARD").distinct(), col("min2") === col("MIN_PRODUCT_UNIT_STANDARD"), "left")
        //                .filter(col("MIN_PRODUCT_UNIT_STANDARD").isNull)
        //                .show(40, false)

        println("sum")
        gycxErd.agg(sum("SALES")).show(false)
        temp.agg(sum("VALUE")).show()

    }

    test("check hosp") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
        sd.sc.setLogLevel("error")
        val prodErd = {
            sd.setUtil(readParquet())
                    .readParquet("hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73").filter(col("MARKET") === "CNS_R")
                    .withColumn("min2", trim(regexp_replace(concat(col("ETC_PRODUCT_NAME"), col("ETC_DOSAGE_NAME"), col("ETC_PACKAGE_DES"), col("ETC_PACKAGE_NUMBER"), col("ETC_CORP_NAME")), " ", "")))
        }
        val hospErd = {
            sd.setUtil(readParquet())
                    .readParquet("hdfs:///repository/hosp_dis_max")
                    .filter("PHA_IS_REPEAT == 0")
                    .dropDuplicates(List("PHA_HOSP_ID"))
        }


        val panelErd = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/Clean/panel/pfizer/CNS_R_NEW")

        val panelTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Panel/0db067e5-c3f2-4112-93be-f447f2fede74")


        val a = panelErd.selectExpr("HOSPITAL_ID as hosp", "SALES as VALUE", "PRODUCT_ID")
                .join(hospErd, col("hosp") === col("HOSPITAL_ID"))
                .join(prodErd.select("min2", "ETC_PRODUCT_ID"), col("PRODUCT_ID") === col("ETC_PRODUCT_ID"))
                .select("VALUE","min2","PHA_HOSP_ID")
                .join(panelTrueDF, col("PHA_HOSP_ID") === col("HOSP_ID") && col("min2") === col("Prod_Name"), "left")
                .drop("PHA_HOSP_ID")
                .distinct()

        val res = a.withColumn("check", expr("abs(SALES - VALUE)")).filter("check > 0.01")
        println(panelErd.count())
        println(a.count())
        println(res.count())
        res.show(false)


    }

    test("test service max") {
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

        val jobs = Config.readJobConfig("pharbers_config/service/servier_Max.yaml")
        val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
        val result = phJobs.head.perform(PhMapArgs(Map.empty))

        val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
//        sd.setUtil(save2Parquet()).save2Parquet(maxDF, "hdfs:///test/dcs/maxTest/CNS_R_NEW2")

//        val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/41ce8fe8-a402-4d5d-88f1-ffcb62117532")
//
        println(maxDF.count())
//        println(maxTrueDF.count())
//
        println(maxDF.agg(sum("f_units")).first.get(0))
        println(maxDF.agg(sum("f_sales")).first.get(0))
//
//        println(maxTrueDF.agg(sum("f_units")).first.get(0))
//        println(maxTrueDF.agg(sum("f_sales")).first.get(0))
    }

    test("check max"){
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
        sd.sc.setLogLevel("error")
//        val maxDFNew = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/maxTest/CNS_R_NEW2")
//        val maxTrueDFNew = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/41ce8fe8-a402-4d5d-88f1-ffcb62117532")
//
//        println(maxDFNew.agg(sum("f_units")).first.get(0))
//        println(maxDFNew.agg(sum("f_sales")).first.get(0))
//
//        println(maxTrueDFNew.agg(sum("f_units")).first.get(0))
//        println(maxTrueDFNew.agg(sum("f_sales")).first.get(0))

        val prodErd = {
            sd.setUtil(readParquet())
                    .readParquet("hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73").filter(col("MARKET") === "CNS_R")
                    .withColumn("min2", trim(regexp_replace(concat(col("ETC_PRODUCT_NAME"), col("ETC_DOSAGE_NAME"), col("ETC_PACKAGE_DES"), col("ETC_PACKAGE_NUMBER"), col("ETC_CORP_NAME")), " ", "")))
        }
        val hospErd = {
            sd.setUtil(readParquet())
                    .readParquet("hdfs:///repository/hosp_dis_max")
                    //                    .readParquet("hdfs:///repository/hosp")
                    .filter("PHA_IS_REPEAT == 0")
                    .dropDuplicates(List("PHA_HOSP_ID"))
        }

        val maxDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/maxTest/CNS_R_NEW")
        val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/41ce8fe8-a402-4d5d-88f1-ffcb62117532")


        val a = maxDF.selectExpr("HOSPITAL_ID as hosp", "f_sales as VALUE", "PRODUCT_ID")
                .join(hospErd, col("hosp") === col("HOSPITAL_ID"))
                .join(prodErd.select("min2", "ETC_PRODUCT_ID"), col("PRODUCT_ID") === col("ETC_PRODUCT_ID"))
                .select("VALUE","min2","PHA_HOSP_ID")
                //                .select("PHA_HOSP_ID")
                .join(maxTrueDF, col("Panel_ID") === col("PHA_HOSP_ID") && col("min2") === regexp_replace(col("Product"), " ", ""), "left")
//                .drop("PHA_HOSP_ID")
                .distinct()
                .cache()
        val res = a.filter("Panel_ID is not null").withColumn("check", expr("abs(f_sales - VALUE)")).filter("check > 100")
        println(res.count())

        res.filter("PHA_HOSP_ID != 'PHA0012671'").sort(col("check").desc).show(false)
        println(res.select("PHA_HOSP_ID").distinct().count())
        println(res.select("min2").distinct().count())


//        println(maxDF.count())
//        println(maxTrueDF.count())
//
//        println(maxDF.agg(sum("f_units")).first.get(0))
//        println(maxDF.agg(sum("f_sales")).first.get(0))
//
//        println(maxTrueDF.agg(sum("f_units")).first.get(0))
//        println(maxTrueDF.agg(sum("f_sales")).first.get(0))
    }
}
