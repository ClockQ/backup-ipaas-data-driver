package com.pharbers.ipaas.data.driver.config.yamlModel

import scala.beans.BeanProperty

/** job配置实体，读取yaml或json生成，包含在action配置中
  *
  * @author dcs
  */
class Operator {
    @BeanProperty var name = ""
    @BeanProperty var factory = ""
    @BeanProperty var oper = ""
    @BeanProperty var args: java.util.Map[String, String] = _
    @BeanProperty var plugins: java.util.List[Plugin] = _
}
