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

package com.pharbers.ipaas.data.driver.run.astellas

import java.io.{File, PrintWriter}
import java.util

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet, save2Parquet}
import env.configObj.{inst, readJobConfig}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.sum
import org.scalatest.FunSuite
import org.yaml.snakeyaml.Yaml

import scala.io.Source

class TestAstellasPanel extends FunSuite {
    implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
    sd.addJar("target/ipaas-data-driver-0.1.jar")
    sd.sc.setLogLevel("ERROR")
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

    test("test astellas gycx clean") {
        def cleanGycx(shouldSave: Boolean, name: String, gycxPath: String, ProductMatchPath: String, ProductERDPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/astellasCleanGycx.yaml"
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
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/Clean/gycx/astellas")
        val resultDF = cleanGycx(
            false,
            "astellasGycxTest",
            "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_Gycx_20180703.csv",
            "hdfs:///data/astellas/pha_config_repository1804/Astellas_ProductMatchTable_20180703.csv",
            "hdfs:///repository/prod_etc_dis_max/astellas",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "hdfs:///test/dcs/Clean/gycx/TestAstellas")

        val checkUnits = checkDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        val cleanUnits = resultDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        println(checkUnits)
        println(cleanUnits)
        assert(Math.abs(checkUnits - cleanUnits) < (cleanUnits * 0.001))

        val checkSales = checkDF.agg(sum("SALES")).first.get(0).toString.toDouble
        val resultSales = resultDF.agg(sum("SALES")).first.get(0).toString.toDouble
        println(checkSales)
        println(resultSales)
        assert(Math.abs(checkSales - resultSales) < (resultSales * 0.001))
    }

    test("test astellas cpa clean") {
        def cleanCpa(shouldSave: Boolean, name: String, cpaPath: String, ProductMatchPath: String, ProductERDPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/astellasCleanCpa.yaml"
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
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/Clean/cpa/astellas")
        val resultDF = cleanCpa(
            false,
            "astellasGycxTest",
            "hdfs:///data/astellas/pha_config_repository1804/Astellas_201804_CPA.csv",
            "hdfs:///data/astellas/pha_config_repository1804/Astellas_ProductMatchTable_20180703.csv",
            "hdfs:///repository/prod_etc_dis_max/astellas",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "hdfs:///test/dcs/Clean/gycx/TestCpa")

        val checkUnits = checkDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        val cleanUnits = resultDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        println(checkUnits)
        println(cleanUnits)
        assert(Math.abs(checkUnits - cleanUnits) < (cleanUnits * 0.001))

        val checkSales = checkDF.agg(sum("SALES")).first.get(0).toString.toDouble
        val resultSales = resultDF.agg(sum("SALES")).first.get(0).toString.toDouble
        println(checkSales)
        println(resultSales)
        assert(Math.abs(checkSales - resultSales) < (resultSales * 0.001))
    }

    test("test astellas sample cpa hosp clean") {
        def cleanSampleCpaHosp(shouldSave: Boolean, name: String, sampleHospPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/astellasCleanSampleCpaHosp.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("sampleHospPath" -> sampleHospPath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("sample_hosp").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val markets = List("Allelock", "Mycamine", "Prograf", "Perdipine", "Harnal", "Gout", "Vesicare", "Grafalon")

        markets.foreach(x => {
            val checkDF = sd.setUtil(readCsv()).readCsv(s"hdfs:///data/astellas/pha_config_repository1804/Astellas_2018_If_panel_all_${x}_20180703.csv")
            val resultDF = cleanSampleCpaHosp(
                false,
                "astellasSampleCpaHospTest",
                s"hdfs:///data/astellas/pha_config_repository1804/Astellas_2018_If_panel_all_${x}_20180703.csv",
                "hdfs:///repository/hosp_dis_max",
                "hdfs:///repository/pha",
                s"hdfs:///test/dcs/Clean/sampleHosp/astellas/${x}_cpa")


            val checkCount = checkDF.filter("HOSP_ID IS NOT NULL AND IF_PANEL_ALL != 0").count()
            val cleanCount = resultDF.count()
            println(checkCount)
            println(cleanCount)

            assert(cleanCount > 20)
        })
    }

    test("test astellas sample gycx hosp clean") {
        def cleanSampleCpaHosp(shouldSave: Boolean, name: String, sampleHospPath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/astellasCleanSampleGycxHosp.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("sampleHospPath" -> sampleHospPath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("sample_hosp").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val markets = List("Allelock", "Mycamine", "Prograf", "Perdipine", "Harnal", "Gout", "Vesicare", "Grafalon")

        markets.foreach(x => {
            val checkDF = sd.setUtil(readCsv()).readCsv(s"hdfs:///data/astellas/pha_config_repository1804/Astellas_2018_If_panel_all_${x}_20180703.csv")
            val resultDF = cleanSampleCpaHosp(
                false,
                "astellasSampleCpaHospTest",
                s"hdfs:///data/astellas/pha_config_repository1804/Astellas_2018_If_panel_all_${x}_20180703.csv",
                "hdfs:///repository/hosp_dis_max",
                "hdfs:///repository/pha",
                s"hdfs:///test/dcs/Clean/sampleHosp/astellas/${x}_gycx")


            val checkCount = checkDF.filter("HOSP_ID IS NOT NULL AND IF_PANEL_ALL != 0").count()
            val cleanCount = resultDF.count()
            println(checkCount)
            println(cleanCount)

            assert(cleanCount > 20)
        })
    }

    test("test astellas threeSource cpa hosp clean") {
        def cleanThreeSource(shouldSave: Boolean, name: String, threeSourceTablePath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/astellasCleanCpaSourceTable.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("threeSourceTablePath" -> threeSourceTablePath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("result").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/Clean/treeSource/astellas/cpa")
        val resultDF = cleanThreeSource(
            false,
            "astellasCleanThreeSourceCpaTest",
            "hdfs:///data/astellas/pha_config_repository1804/Astellas_ThreeSourceTable_20180629.csv",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "hdfs:///test/dcs/Clean/treeSource/astellas/cpa")


        val checkCount = checkDF.count()
        val resultCount = resultDF.count()
        println(checkCount)
        println(resultCount)
        assert(checkCount == resultCount)
    }

    test("test astellas threeSource gycx hosp clean") {
        def cleanThreeSource(shouldSave: Boolean, name: String, threeSourceTablePath: String, hospERDPath: String, PhaPath: String, savePath: String): DataFrame = {
            val templatePath = "src/test/maxConfig/template/astellasCleanGycxSourceTable.yaml"
            val yamlPath = buildYaml(templatePath,
                Map("threeSourceTablePath" -> threeSourceTablePath,
                    "hospERDPath" -> hospERDPath,
                    "phaERDPath" -> PhaPath),
                name)
            val phJobs = inst(readJobConfig(yamlPath))
            val result = phJobs.head.perform(PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd),
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
            )))

            val cleanDF = result.toMapArgs[PhDFArgs].get("result").get

            if (shouldSave) {
                sd.setUtil(save2Parquet()).save2Parquet(cleanDF, savePath, SaveMode.Overwrite)
            }
            cleanDF
        }

        val checkDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/dcs/Clean/treeSource/astellas/gycx")
        val resultDF = cleanThreeSource(
            false,
            "astellasCleanThreeSourceCpaTest",
            "hdfs:///data/astellas/pha_config_repository1804/Astellas_ThreeSourceTable_20180629.csv",
            "hdfs:///repository/hosp_dis_max",
            "hdfs:///repository/pha",
            "hdfs:///test/dcs/Clean/treeSource/astellas/gycx")


        val checkCount = checkDF.count()
        val resultCount = resultDF.count()
        println(checkCount)
        println(resultCount)
        assert(checkCount == resultCount)
    }

}
