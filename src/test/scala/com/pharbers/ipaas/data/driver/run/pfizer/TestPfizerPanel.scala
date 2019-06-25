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
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, save2Parquet}
import env.configObj.{inst, readJobConfig}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.yaml.snakeyaml.Yaml

import scala.io.Source


class TestPfizerPanel extends FunSuite {
    implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")
    sd.addJar("target/ipaas-data-driver-0.1.jar")
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
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
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
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
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
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
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
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
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

        assert(Math.abs(checkCount - cleanCount) > -1)
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
                "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
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

        assert(Math.abs(checkCount - cleanCount) > -1)
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

class test extends FunSuite {
    test("") {
        import scala.collection.JavaConversions._
        val data: java.util.Map[String, java.util.Map[String, String]] = new util.HashMap()
        data.put("args", Map(
            "gycxPath" -> "&gycxPath hdfs:///data/pfizer/pha_config_repository1804/Pfizer_201804_Gycx.csv",
            "hospERDPath" -> "&hospERDPath hdfs:///repository/pha",
            "productERDPath" -> "&productERDPath hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73",
            "phaERDPAth" -> "&phaERDPAth hdfs:///repository/pha",
            "ProductMatchPath" -> "&ProductMatchPath hdfs:///data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"
        ))

        val head = new Yaml().dumpAsMap(data).replace("'", "")
        val file = new File("src/test/maxConfig/test.yaml")
        file.deleteOnExit()
        file.createNewFile()
        val writer = new PrintWriter(file)
        writer.println(head)
        writer.println(Source.fromFile("src/test/maxConfig/template/pfizerCleanGycx.yaml").mkString)
        writer.close()
        val a = inst(readJobConfig("src/test/maxConfig/test.yaml"))
        println(a)
    }
}
