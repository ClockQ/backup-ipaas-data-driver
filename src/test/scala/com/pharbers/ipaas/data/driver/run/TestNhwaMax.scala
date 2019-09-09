package com.pharbers.ipaas.data.driver.run

import env.configObj._
import test.tag.MaxTag
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet}
import org.apache.spark.launcher.SparkLauncher

@MaxTag
class TestNhwaMax extends FunSuite {

    import env.sparkObj._

    test("test nhwa MZ clean") {
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZclean.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map()))

        val cleanDF = result.toMapArgs[PhDFArgs].get("cleanResult").get
        val cleanTrueDF = sparkDriver.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc")

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
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZpanelByCpa.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map()))

        val panelDF = result.toMapArgs[PhDFArgs].get("panelResult").get
        val panelTrueDF = sparkDriver.setUtil(readCsv()).readCsv("hdfs:///test/qi/qi/1809_panel.csv")

        panelDF.show(false)
        panelTrueDF.show(false)

        val panelDFCount = panelDF.count()
        val panelTrueDFCount = panelTrueDF.count()
        println(panelDFCount)
        println(panelTrueDFCount)

        val panelDFUnits = panelDF.agg(sum("UNITS")).first.get(0).toString.toDouble
        val panelTrueDFUnits = panelTrueDF.agg(sum("Units")).first.get(0).toString.toDouble
        println(panelDFUnits)
        println(panelTrueDFUnits)
        assert(Math.abs(panelDFUnits - panelTrueDFUnits) < panelTrueDFUnits * 0.01)

        val panelDFSales = panelDF.agg(sum("SALES")).first.get(0).toString.toDouble
        val panelTrueDFSales = panelTrueDF.agg(sum("Sales")).first.get(0).toString.toDouble
        println(panelDFSales)
        println(panelTrueDFSales)
        assert(Math.abs(panelDFSales - panelTrueDFSales) < panelTrueDFSales * 0.01)
    }

    test("test nhwa MZ max") {
        val phJobs = inst(readJobConfig("src/test/max_config/nhwa/MZmax.yaml"))
        val result = phJobs.head.perform(PhMapArgs(Map()))

        val maxDF = result.toMapArgs[PhDFArgs].get("maxResult").get
        val maxTrueDF = sparkDriver.setUtil(readParquet()).readParquet("hdfs:///test/qi/qi/new_max_true")

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

    test("submit") {
        import org.apache.spark.launcher.SparkAppHandle
        import java.util.concurrent.CountDownLatch
        val countDownLatch = new CountDownLatch(1)
        val sparkLauncher = new SparkLauncher()
                .setMainClass("com.pharbers.ipaas.data.driver.Main")
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setConf("spark.executor.memory", "2G")
                .setConf("spark.yarn.appMasterEnv.PHA_CONF_HOME", "")
                .addFile("D:code/config/kafka_config.xml")
                .addFile("D:code/config/kafka.broker1.truststore.jks")
                .addFile("D:code/config/kafka.broker1.keystore.jks")
                .addFile("C:UsersEDZDesktopMZmax.yaml")
                .setAppResource("D:codepharbersipaas-data-drivertargetjob-context.jar")
                .setSparkHome("D:spark-2.3.0-bin-hadoop2.7")
                .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, "./__app__.jar")
                .addSparkArg("--num-executors", "1")
                .addSparkArg("--queue", "default")
                .addAppArgs("yaml", "LOCAL", "MZmax.yaml", "test")
        sparkLauncher.startApplication(
            new SparkAppHandle.Listener() {
                override def stateChanged(handle: SparkAppHandle): Unit = {
                    if (handle.getState.isFinal) {
                        countDownLatch.countDown()
                    }
                    println("state:" + handle.getState.toString)
                }

                override def infoChanged(handle: SparkAppHandle): Unit = {
                    println("Info:" + handle.getState.toString)
                }
            }
        )
        countDownLatch.await()
        println("end")
    }

    test("max submit") {
        import org.apache.spark.launcher.SparkAppHandle
        import java.util.concurrent.CountDownLatch
        val countDownLatch = new CountDownLatch(1)
        val sparkLauncher = new SparkLauncher()
                .setMainClass("com.pharbers.max.submit.Main")
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setConf("spark.executor.memory", "2G")
                .setConf("spark.yarn.appMasterEnv.PHA_CONF_HOME", "")
                .addFile("hdfs:///test/dcs/job/kafka_config.xml")
                .addFile("hdfs:///test/dcs/job/kafka.broker1.truststore.jks")
                .addFile("hdfs:///test/dcs/job/kafka.broker1.keystore.jks")
                .setAppResource("hdfs:///test/dcs/job/max-context.jar")
                .setSparkHome("D:\\spark-2.3.3-bin-hadoop2.7")
                .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, "./__app__.jar")
                .addSparkArg("--num-executors", "1")
                .addSparkArg("--queue", "default")
                .addAppArgs("hdfs:///test/dcs/job/MZmax.yaml", "test", """{\"companyId\":\"'5ca069bceeefcc012918ec72'\",\"cpa\":\"hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_CPA_20181126.csv\",\"mkt\":\"MARKET == '麻醉市场' AND SAMPLE == 1\",\"ym\":\"YM == 201809\",\"date\":\"DATE == 201809\",\"missHosp\":\"hdfs:///repository/miss_hosp/5ca069bceeefcc012918ec72\",\"notPublishHosp\":\"hdfs:///repository/not_published_hosp/5ca069bceeefcc012918ec72\",\"fullHosp\":\"hdfs:///repository/full_hosp/5ca069bceeefcc012918ec72/20180629\",\"sampleHosp\":\"hdfs:///repository/sample_hosp/5ca069bceeefcc012918ec72/mz\",\"universe\":\"hdfs:///repository/universe_hosp/5ca069bceeefcc012918ec72/mz\"}""")
        sparkLauncher.startApplication(
            new SparkAppHandle.Listener() {
                override def stateChanged(handle: SparkAppHandle): Unit = {
                    if (handle.getState.isFinal) {
                        countDownLatch.countDown()
                    }
                    println("state:" + handle.getState.toString)
                }

                override def infoChanged(handle: SparkAppHandle): Unit = {
                    println("Info:" + handle.getState.toString)
                }
            }
        )
        countDownLatch.await()
        println("end")
    }
}
