package com.pharbers.ipaas.data.driver.config.yamlModel

import scala.beans.BeanProperty

/** action配置实体，读取yaml或json生成,包含在job配置中
  *
  * @author dcs
  */
class Action {
    @BeanProperty var name = ""
    @BeanProperty var factory = ""
    @BeanProperty val args: java.util.HashMap[String, String] = null
    @BeanProperty val opers: java.util.List[Operator] = null
}
