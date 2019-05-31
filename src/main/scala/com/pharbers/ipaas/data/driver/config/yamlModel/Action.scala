package com.pharbers.ipaas.data.driver.config.yamlModel

import scala.beans.BeanProperty

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
class Action {
    @BeanProperty var name = ""
    @BeanProperty var factory = ""
    @BeanProperty val args: java.util.HashMap[String, String] = null
    @BeanProperty val opers: java.util.List[Operator] = null
}
