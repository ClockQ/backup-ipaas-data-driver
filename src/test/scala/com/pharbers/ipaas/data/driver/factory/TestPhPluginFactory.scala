package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import org.scalatest.FunSuite

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
class TestPhPluginFactory extends FunSuite{
    test("PhPluginFactory"){
        val addByWhen = PhPluginFactory().inst(PhMapArgs(Map("name" -> PhStringArgs("com.pharbers.ipaas.data.driver.plugin.BaseCalcPlugin"))))
        assert(addByWhen.name == "CalcPlugin")
    }
}
