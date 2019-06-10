package com.pharbers.ipaas.data.driver.config

import java.io.{File, FileInputStream}

import com.pharbers.ipaas.data.driver.config.yamlModel._
import org.scalatest.FunSuite


/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
class testYamlReader extends FunSuite{
    test("yaml to job"){
        val stream = new FileInputStream(new File("D:\\code\\pharbers\\ipaas-data-driver\\pharbers_config\\testYAML.yaml"))
        val jobs = YamlReader().readObjects[Job](stream)
        assert(!jobs.head.getName.isEmpty)
    }
}
