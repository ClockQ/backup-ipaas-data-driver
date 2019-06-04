package com.pharbers.ipaas.data.driver.config

import java.io.{File, FileInputStream}

import com.pharbers.ipaas.data.driver.config.yamlModel._
import org.scalatest.FunSuite


/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
class testJsonReader extends FunSuite{
    test("json read"){
        val stream = new FileInputStream(new File("D:\\code\\pharbers\\ipaas-data-driver\\pharbers_config\\testJson.json"))
        val jobs = JsonReader().readObjects[JobBean](stream)
        assert(!jobs.head.getName.isEmpty)
    }
}
