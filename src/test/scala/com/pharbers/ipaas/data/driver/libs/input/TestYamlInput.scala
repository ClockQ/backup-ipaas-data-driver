package com.pharbers.ipaas.data.driver.libs.input

import java.io._

import org.scalatest.FunSuite
import com.pharbers.ipaas.data.driver.api.model.{Job, Plugin}

/**
  * @author clock
  * @version 0.1
  * @since 2019/06/14 11:15
  * @note
  */
class TestYamlInput extends FunSuite{

    test("yaml input from memory to Plugin"){
        val data =
            """
              |name: testName
              |factory: testFactory
              |sub:
              |  name: subTestName
              |  factory: testFactory
            """.stripMargin
        val stream = new ByteArrayInputStream(data.getBytes)
        val plugin = YamlInput().readObject[Plugin](stream)
        assert("testName" == plugin.name)
        assert("testFactory" == plugin.factory)
        assert("subTestName" == plugin.sub.name)
    }

    test("yaml input from memory to Plugins"){
        val data =
            """
              |name: testName1
              |factory: testFactory1
              |---
              |name: testName2
              |factory: testFactory2
            """.stripMargin
        val stream = new ByteArrayInputStream(data.getBytes)
        val plugin = YamlInput().readObjects[Plugin](stream)
        assert("testName1" == plugin.head.name)
        assert("testFactory1" == plugin.head.factory)
        assert("testName2" == plugin(1).name)
        assert("testFactory2" == plugin(1).factory)
    }

    test("yaml input from file to Jobs") {
        val stream = new FileInputStream(new File("src/test/scala/com/pharbers/ipaas/data/driver/libs/input/testYaml.yaml"))
        val jobs = YamlInput().readObjects[Job](stream)
        assert(jobs.size == 2)
        assert(!jobs.head.getName.isEmpty)
    }
}
