package com.pharbers.ipaas.data.driver.config

import java.io.{File, FileInputStream}

import com.pharbers.ipaas.data.driver.config.yamlModel.JobBean
import org.scalatest.FunSuite

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
class TestConfig extends FunSuite{

    test("read job config"){
        val yamlJobs = Config.readJobConfig("pharbers_config/testYAML.yaml")
        val jsonJobs =  Config.readJobConfig("pharbers_config/testJson.json")
        assert(yamlJobs.head.getName == jsonJobs.head.getName)
    }

    test("yaml to job"){
        val stream = new FileInputStream(new File("pharbers_config/testYAML.yaml"))
        val jobs = YamlReader().readObjects[JobBean](stream)
        assert(jobs.head.getName == "clean" && jobs.tail.head.getName == "clean2")
    }

    test("json to job"){
        val stream = new FileInputStream(new File("pharbers_config/testJson.json"))
        val jobs = JsonReader().readObjects[JobBean](stream)
        assert(jobs.head.getName == "clean")
    }

}
