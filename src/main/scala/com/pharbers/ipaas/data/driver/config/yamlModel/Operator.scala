package com.pharbers.ipaas.data.driver.config.yamlModel

import scala.beans.BeanProperty

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
class Operator {
    @BeanProperty var name = ""
    @BeanProperty var factory = ""
    @BeanProperty var oper = ""
    @BeanProperty var args: java.util.Map[String, String] = _
    @BeanProperty var plugins: java.util.List[Plugin] = _
}
