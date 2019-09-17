package com.pharbers.ipaas.data.driver.operators.util

import com.pharbers.ipaas.data.driver.api.Annotation.{Operator, Plugin}
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/09/16 19:24
  * @note 一些值得注意的地方
  */
@Operator(args = Array("test"))
@Plugin(args = Array("testPlugin"))
class TestOperatorInterpreter extends FunSuite {
    test("TestOperatorInterpreter"){
        val operatorAnnotations = OperatorInterpreter.getOperatorClass("com.pharbers.ipaas.data.driver.operators.util", classOf[Operator])
        assert(operatorAnnotations.head._1 == "com.pharbers.ipaas.data.driver.operators.util.TestOperatorInterpreter")
        assert(operatorAnnotations.head._2.args().head == "test")
        val operatorAnnotations2 = OperatorInterpreter.getOperatorClass("com.pharbers.ipaas.data.driver.operators", classOf[Operator], true)
        assert(operatorAnnotations2.head._1 == "com.pharbers.ipaas.data.driver.operators.util.TestOperatorInterpreter")
        assert(operatorAnnotations2.head._2.args().head == "test")
        val pluginAnnotations = OperatorInterpreter.getOperatorClass("com.pharbers.ipaas.data.driver.operators.util", classOf[Plugin])
        assert(pluginAnnotations.head._1 == "com.pharbers.ipaas.data.driver.operators.util.TestOperatorInterpreter")
        assert(pluginAnnotations.head._2.args().head == "testPlugin")
    }
}
