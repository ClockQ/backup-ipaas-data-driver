package com.pharbers.ipaas.data.driver.libs.input

import java.io._

import org.scalatest.FunSuite
import com.pharbers.ipaas.data.driver.api.model.{Job, Plugin}

/**
  * @author clock
  * @version 0.1
  * @since 2019/06/14 11:13
  * @note
  */
class TestJsonInput extends FunSuite {

    test("json input from memory to Plugin") {
        val data =
            """
              |{
              |    "name" : "testName",
              |    "factory" : "testFactory"
              |}
            """.stripMargin
        val stream = new ByteArrayInputStream(data.getBytes)
        val plugin = JsonInput().readObject[Plugin](stream)
        assert("testName" == plugin.name)
        assert("testFactory" == plugin.factory)
    }

    test("json input from memory to Plugins") {
        val data =
            """
              |[
              |    {
              |        "name" : "testName1",
              |        "factory" : "testFactory1"
              |    },
              |    {
              |        "name" : "testName2",
              |        "factory" : "testFactory2"
              |    }
              |]
            """.stripMargin
        val stream = new ByteArrayInputStream(data.getBytes)
        val plugin = JsonInput().readObjects[Plugin](stream)
        assert("testName1" == plugin.head.name)
        assert("testFactory1" == plugin.head.factory)
        assert("testName2" == plugin(1).name)
        assert("testFactory2" == plugin(1).factory)
    }

    test("json input from file to Jobs") {
        val stream = new FileInputStream(new File("C:\\Users\\EDZ\\Documents\\WeChat Files\\dengcao1993\\FileStorage\\File\\2019-08\\pressureTest.json"))
        val jobs = JsonInput().readObjects[Job](stream)
        assert(jobs.size == 1)
        assert(!jobs.head.getName.isEmpty)
    }
}
