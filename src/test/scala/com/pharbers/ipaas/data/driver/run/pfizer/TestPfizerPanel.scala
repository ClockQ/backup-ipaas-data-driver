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

package com.pharbers.ipaas.data.driver.run.pfizer

import java.io._
import java.util
import org.scalatest.FunSuite
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogFormat, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet, save2Parquet}
import env.configObj.{inst, readJobConfig}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.yaml.snakeyaml.Yaml
import scala.io.Source


class TestPfizerPanel extends FunSuite {
    implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
//    sd.addJar("target/ipaas-data-driver-0.1.jar")
    sd.sc.setLogLevel("ERROR")
    test("test pfizer gycx clean") {
        def cleanGycx(shouldSave: Boolean, name: String, gycxPath: String, ProductMatchPath: String, ProductERDPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerCleanGycx.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("gycxPath" -> gycxPath,
                    "hospERDPath" -> hospERDPath,
                    "productERDPath" -> ProductERDPath,
                    "phaERDPath" -> PhaPath,
                    "ProductMatchPath" -> ProductMatchPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_Gycx.csv")
        val resultDF = cleanGycx(
            false,
            "pfizerGycxTest",
            "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_Gycx.csv",
            "hdfs:///data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv",
            "hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "hdfs:///test/dcs/Clean/gycx/pfizer")

        val checkUnits = checkDF.agg(sum("STANDARD_UNIT")).first.get(0).toString.toDouble
        val cleanUnits = resultDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        println(checkUnits)
        println(cleanUnits)
        assert(Math.abs(checkUnits - cleanUnits) < (cleanUnits * 0.01))

        val checkSales = checkDF.agg(sum("VALUE")).first.get(0).toString.toDouble
        val resultSales = resultDF.agg(sum("SALES")).first.get(0).toString.toDouble
        println(checkSales)
        println(resultSales)
        assert(Math.abs(checkSales - resultSales) < (resultSales * 0.01))
    }

    test("test pfizer cpa clean") {
        def cleanCpa(shouldSave: Boolean, name: String, cpaPath: String, ProductMatchPath: String, ProductERDPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerCleanCpa.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("cpaPath" -> cpaPath,
                    "hospERDPath" -> hospERDPath,
                    "productERDPath" -> ProductERDPath,
                    "phaERDPath" -> PhaPath,
                    "ProductMatchPath" -> ProductMatchPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_CPA.csv")
        val resultDF = cleanCpa(
            false,
            "pfizerCpaTest",
            "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_CPA.csv",
            "hdfs:///data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv",
            "hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "hdfs:///test/dcs/Clean/cpa/pfizer")

        val checkUnits = checkDF.agg(sum("STANDARD_UNIT")).first.get(0).toString.toDouble
        val cleanUnits = resultDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        println(checkUnits)
        println(cleanUnits)
        assert(Math.abs(checkUnits - cleanUnits) < (cleanUnits * 0.01))

        val checkSales = checkDF.agg(sum("VALUE")).first.get(0).toString.toDouble
        val resultSales = resultDF.agg(sum("SALES")).first.get(0).toString.toDouble
        println(checkSales)
        println(resultSales)
        assert(Math.abs(checkSales - resultSales) < (resultSales * 0.01))
    }

    test("test pfizer full hosp clean") {
        def cleanHullHosp(shouldSave: Boolean, name: String, fullHospPath: String, ProductMatchPath: String, ProductERDPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerCleanFullHosp.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("fullHospPath" -> fullHospPath,
                    "hospERDPath" -> hospERDPath,
                    "productERDPath" -> ProductERDPath,
                    "phaERDPath" -> PhaPath,
                    "ProductMatchPath" -> ProductMatchPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_FullHosp.txt", 31.toChar.toString)
        val resultDF = cleanHullHosp(
            false,
            "pfizerFullHospTest",
            "hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_FullHosp.txt",
            "hdfs:///data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv",
            "hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "hdfs:///test/dcs/Clean/fullHosp/pfizer")

        val checkUnits = checkDF.agg(sum("STANDARD_UNIT")).first.get(0).toString.toDouble
        val cleanUnits = resultDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        println(checkUnits)
        println(cleanUnits)
        assert(Math.abs(checkUnits - cleanUnits) < (cleanUnits * 0.01))

        val checkSales = checkDF.agg(sum("VALUE")).first.get(0).toString.toDouble
        val resultSales = resultDF.agg(sum("SALES")).first.get(0).toString.toDouble
        println(checkSales)
        println(resultSales)
        assert(Math.abs(checkSales - resultSales) < (resultSales * 0.01))
    }

    test("test pfizer miss hosp clean") {
        def cleanMissHosp(shouldSave: Boolean, name: String, missHospPath: String, hospERDPath: String, PhaPath: String, year: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerCleanMissHosp.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("missHospPath" -> missHospPath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath,
                    "year" -> year),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("not_arrival_hosp").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readCsv()).readCsv("hdfs:///data/pfizer/pha_config_repository1804/missingHospital.csv")
        val resultDF = cleanMissHosp(
            false,
            "pfizerMissHospTest",
            "hdfs:///data/pfizer/pha_config_repository1804/missingHospital.csv",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "2018",
            "hdfs:///test/dcs/Clean/MissHosp/pfizer"
        )

        val checkCount = checkDF.count()
        val cleanCount = resultDF.count()
        println(checkCount)
        println(cleanCount)

        assert(cleanCount - checkCount > -1)
    }

    test("test pfizer sample cpa hosp clean") {
        def cleanSampleCpaHosp(shouldSave: Boolean, name: String, sampleHospPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerCleanSampleCpaHosp.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("sampleHospPath" -> sampleHospPath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("sample_hosp").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val markets = List("CNS_R", "CNS_Z", "DVP", "ELIQUIS", "Specialty_other", "Specialty_champix", "Urology_other", "Urology_viagra", "AI_R_other", "AI_R_zith"
            , "AI_S", "AI_W", "HTN", "INF", "LD", "ONC_other", "ONC_aml", "PAIN_other", "PAIN_lyrica", "AI_D", "ZYVOX", "PAIN_C", "HTN2")
        markets.foreach(x => {
            val checkDF = sd.setUtil(readCsv()).readCsv(s"hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_If_panel_all_$x.csv")
            val resultDF = cleanSampleCpaHosp(
                false,
                "pfizerSampleCpaHospTest",
                s"hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_If_panel_all_$x.csv",
                "hdfs:///repository/hosp_dis_max",
                "hdfs:///repository/pha",
                s"hdfs:///test/dcs/Clean/sampleHosp/pfizer/$x" + "_cpa"
            )
            val checkCount = checkDF.filter("HOSP_ID IS NOT NULL AND IF_PANEL_ALL != 0").count()
            val cleanCount = resultDF.count()
            println(checkCount)
            println(cleanCount)

            assert(cleanCount > 20)
        })
    }

    test("test pfizer sample gycx hosp clean") {
        def cleanSampleGycxHosp(shouldSave: Boolean, name: String, sampleHospPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerCleanSampleGycxHosp.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("sampleHospPath" -> sampleHospPath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("sample_hosp").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val markets = List("CNS_R", "CNS_Z", "DVP", "ELIQUIS", "Specialty_other", "Specialty_champix", "Urology_other", "Urology_viagra", "AI_R_other", "AI_R_zith"
            , "AI_S", "AI_W", "HTN", "INF", "LD", "ONC_other", "ONC_aml", "PAIN_other", "PAIN_lyrica", "AI_D", "ZYVOX", "PAIN_C", "HTN2")
        markets.foreach(x => {
            val checkDF = sd.setUtil(readCsv()).readCsv(s"hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_If_panel_all_$x.csv")
            val resultDF = cleanSampleGycxHosp(
                false,
                "pfizerSampleGycxHospTest",
                s"hdfs:///data/pfizer/pha_config_repository1804/Pfizer_2018_If_panel_all_$x.csv",
                "hdfs:///repository/hosp_dis_max",
                "hdfs:///repository/pha",
                s"hdfs:///test/dcs/Clean/sampleHosp/pfizer/$x" + "_gycx"
            )
            val checkCount = checkDF.filter("HOSP_ID IS NOT NULL AND IF_PANEL_ALL != 0").count()
            val cleanCount = resultDF.count()
            println(checkCount)
            println(cleanCount)

            assert(cleanCount > 20)
        })
    }

    test("test pfizer panel") {
        def panel(shouldSave: Boolean, name: String, gycxERDPath: String, cpaERDPath: String, missHospERDPath: String, fullHospERDPath: String, cpaSampleHospERDPath: String, gycxSampleHospERDPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerPanel.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("gycxERDPath" -> gycxERDPath,
                    "cpaERDPath" -> cpaERDPath,
                    "missHospERDPath" -> missHospERDPath,
                    "fullHospERDPath" -> fullHospERDPath,
                    "cpaSampleHospERDPath" -> cpaSampleHospERDPath,
                    "gycxSampleHospERDPath" -> gycxSampleHospERDPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("panelERD").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val markets = Map(
            "a3531d93-830b-4d8a-ac0a-c40a2e738199" -> "CNS_R",
            "6b48e296-8891-4ae2-807b-9da316ee3402" -> "CNS_Z",
            "b8521218-c6ca-411d-81f4-0dbf8bb67f5f" -> "DVP",
            "1b1693b1-3330-4c50-979a-1b8560f05ecd" -> "ELIQUIS",
            "51ee5bc3-9cdc-46b9-a60f-c586209c587c" -> "Specialty_other",
            "f72ff47b-2e7e-4168-8947-00958413755e" -> "Specialty_champix",
            "495a1446-2365-4608-9321-0092b2f59262" -> "Urology_other",
            "6190e088-634e-460a-b228-0b012f9b02cb" -> "Urology_viagra",
            "3444edaf-941f-4976-a229-7a4f5c26aad0" -> "AI_R_other",
            "00e606aa-daab-42ec-821d-bfc6fb55ae5f" -> "AI_R_zith",
            "00174647-cc98-416e-bbb6-2f97417c86aa" -> "AI_S",
            "490ac3b6-914f-47d5-801f-5052d75ede9b" -> "AI_W",
            "68f07d7b-1749-4584-8045-fde03625abf6" -> "HTN",
            "977456bf-7c5a-4efe-85dc-7d25f27c3d63" -> "INF",
            "21018518-5cba-46ef-8d68-197d2dc8c326" -> "LD",
            "800681de-0fc1-4a51-9f14-70c05cea1177" -> "ONC_other",
            "fbc94be1-7052-41d4-9574-9768701510f6" -> "ONC_aml",
            "9ef2f709-896f-4a61-b612-af400ffbf801" -> "PAIN_other",
            "2452c268-c9a6-4b23-ba2f-d57d794ab734" -> "PAIN_lyrica",
            "4a3c58a9-2c34-4c3e-a8ea-d011d0dd4807" -> "AI_D",
            "03657729-379a-4554-94f4-eaa645b4a214" -> "ZYVOX",
            "4d9bf83f-c2fe-41e6-b2bb-d52f11043b09" -> "PAIN_C",
            "546c538b-3495-41fe-b609-c97e1eab1ca5" -> "HTN2"
        )
        markets.foreach(x => {
            println(x._2)
            val checkDF = sd.setUtil(readParquet()).readParquet(s"hdfs:///workData/Panel/${x._1}")
            val resultDF = panel(
                false,
                s"pfizer${x._2}PanelTest",
                "hdfs:///test/dcs/Clean/gycx/pfizer",
                "hdfs:///test/dcs/Clean/cpa/pfizer",
                "hdfs:///test/dcs/Clean/missHosp/pfizer",
                "hdfs:///test/dcs/Clean/fullHosp/pfizer",
                s"hdfs:///test/dcs/Clean/sampleHosp/pfizer/${x._2}" + "_cpa",
                s"hdfs:///test/dcs/Clean/sampleHosp/pfizer/${x._2}" + "_gycx",
                s"hdfs:///test/dcs/Clean/panel/pfizer/${x._2}"
            )

            val checkUnits = checkDF.agg(sum("UNITS")).first.get(0).toString.toDouble
            val cleanUnits = resultDF.agg(sum("UNITS")).first.get(0).toString.toDouble
            println(checkUnits)
            println(cleanUnits)
            if(x._2 != "ELIQUIS"){
                assert(Math.abs(checkUnits - cleanUnits) < (checkUnits * 0.1))
            }

            val checkSales = checkDF.agg(sum("SALES")).first.get(0).toString.toDouble
            val resultSales = resultDF.agg(sum("SALES")).first.get(0).toString.toDouble
            println(checkSales)
            println(resultSales)
            if(x._2 != "ELIQUIS"){
                assert(Math.abs(checkSales - resultSales) < (checkSales * 0.1))
            }
        })
    }

    test("test pfizer universe clean") {
        def cleanUniverse(shouldSave: Boolean, name: String, universePath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/pfizerCleanUniverse.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("universePath" -> universePath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("universeDF").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val markets = List("CNS_R", "CNS_Z", "DVP", "ELIQUIS", "Specialty_other", "Specialty_champix", "Urology_other", "Urology_viagra", "AI_R_other", "AI_R_zith"
            , "AI_S", "AI_W", "HTN", "INF", "LD", "ONC_other", "ONC_aml", "PAIN_other", "PAIN_lyrica", "AI_D", "ZYVOX", "PAIN_C", "HTN2")
        markets.foreach(x => {
            val checkDF = sd.setUtil(readCsv()).readCsv(s"hdfs:///data/pfizer/pha_config_repository1804/Pfizer_Universe_$x.csv")
            val resultDF = cleanUniverse(
                false,
                "pfizerUniverseTest",
                s"hdfs:///data/pfizer/pha_config_repository1804/Pfizer_Universe_$x.csv",
                "hdfs:///repository/hosp_dis_max",
                "hdfs:///repository/pha",
                s"hdfs:///test/dcs/Clean/universe/pfizer/$x"
            )
            val checkCount = checkDF.count()
            val cleanCount = resultDF.count()
            println(checkCount)
            println(cleanCount)

            assert(Math.abs(cleanCount - checkCount) < checkCount * 0.1)
        })
    }

    test("test pfizer max") {
        def max(shouldSave: Boolean, name: String, panelERDPath: String, universeERDPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/commonMax.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("panelERDPath" -> panelERDPath,
                    "universeERDPath" -> universeERDPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val markets = Map(
            "71f06a1f-97e5-4238-a324-7ad2c0765a71" -> "AI_D"
        )
        markets.foreach(x => {
            println(x._2)
            val checkDF = sd.setUtil(readParquet()).readParquet(s"/workData/Max/${x._1}")
            val resultDF = max(
                false,
                s"astellas${x._2}MaxTest",
                s"hdfs:///test/dcs/Clean/panel/pfizer/${x._2}",
                s"hdfs:///test/dcs/Clean/universe/pfizer/${x._2}",
                s"hdfs:///test/dcs/Clean/max/astellas/${x._2}"
            )

            val checkUnits = checkDF.agg(sum("f_units")).first.get(0).toString.toDouble
            val cleanUnits = resultDF.agg(sum("f_units")).first.get(0).toString.toDouble
            println(checkUnits)
            println(cleanUnits)
            assert(Math.abs(checkUnits - cleanUnits) < (checkUnits * 0.1))

            val checkSales = checkDF.agg(sum("f_sales")).first.get(0).toString.toDouble
            val resultSales = resultDF.agg(sum("f_sales")).first.get(0).toString.toDouble
            println(checkSales)
            println(resultSales)
            assert(Math.abs(checkSales - resultSales) < (checkSales * 0.1))
        })
    }


    def buildYaml(templatePath: String, argsMap: Map[String, String], name: String): String = {
        import scala.collection.JavaConversions._
        val data: java.util.Map[String, java.util.Map[String, String]] = new util.HashMap()
        data.put("args", argsMap.map(x => (x._1, "&" + x._1 + " " + x._2)))
        val head = new Yaml().dumpAsMap(data).replace("'", "")
        val file = new File("src/test/maxConfig/" + name + ".yaml")
        file.deleteOnExit()
        file.createNewFile()
        val writer = new PrintWriter(file)
        writer.println(head)
        writer.println(Source.fromFile(templatePath).mkString)
        writer.close()
        file.getPath
    }

}
