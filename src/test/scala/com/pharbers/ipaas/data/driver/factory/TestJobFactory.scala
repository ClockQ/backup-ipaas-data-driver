package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.Config
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readCsv, readParquet}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum
import org.scalatest.FunSuite

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
class TestJobFactory extends FunSuite{
    test("get job from job factory"){
        val jobs = Config.readJobConfig("pharbers_config/max.yaml")
        val phJobs = jobs.map(x => {
            PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst()
        })
        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
        val dfCheck = sd.setUtil(readParquet()).readParquet("hdfs:///test/qi/qi/max_true")
//        val dfCheck = sd.setUtil(readCsv()).readCsv("hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_Offline_MaxResult_20181126.csv")
        val df = phJobs.head.perform(PhMapArgs(Map.empty))
//        df.toMapArgs[PhDFArgs].get.foreach(x => {
//            println(x._1)
//            x._2.get.show(false)
//        })
        df.toMapArgs[PhDFArgs].get("maxResultDF").get.show(false)
        val maxDF = df.toMapArgs[PhDFArgs].get("maxResultDF").get
        println(dfCheck.agg(sum("f_units")).first.get(0))
        println(dfCheck.agg(sum("f_sales")).first.get(0))

        println(maxDF.agg(sum("f_units")).first.get(0))
        println(maxDF.agg(sum("f_sales")).first.get(0))
    }
}
