package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.Config
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
class TestJobFactory extends FunSuite{
    test("get job from job factory"){
        val jobs = Config.readJobConfig("pharbers_config/testYAML.yaml")
        val phJobs = jobs.map(x => {
            PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst()
        })
//        implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")
//        val df = sd.setUtil(readCsv()).readCsv("/test/dcs/201801_201901_max_result_test.csv")
        val df = phJobs.head.perform(PhMapArgs(Map.empty))
//        df.toMapArgs[PhDFArgs].get.foreach(x => {
//            println(x._1)
//            x._2.get.show(false)
//        })
        df.toMapArgs[PhDFArgs].get("cpaMatchHosp").get.show(false)
    }
}
