package com.pharbers.ipaas.data.driver.libs.read

import org.scalatest.FunSuite

/**
 * @author clock
 * @version 0.2
 * @since 2019/08/28 10:46
 * @note
 */
class TestStringRead extends FunSuite {
    test("读取字符串到输入流") {
        val data =
            """
              |{
              |    "name" : "testName",
              |    "factory" : "testFactory"
              |}
            """.stripMargin
        val result = StringRead(data).toInputStream()
        assert(result != null)
    }
}
