package com.pharbers.ipaas.data.driver.config.yamlModel

import com.pharbers.ipaas.data.driver.api.work.PhStringArgs

import scala.beans.BeanProperty

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
class Job(){
//    @BeanProperty
    var name: PhStringArgs = PhStringArgs("")
    @BeanProperty
    var factory: String = ""
    @BeanProperty
    var actions: java.util.List[Action] = _
    def setName(name: String): Unit ={
        this.name = PhStringArgs(name)
    }
}
