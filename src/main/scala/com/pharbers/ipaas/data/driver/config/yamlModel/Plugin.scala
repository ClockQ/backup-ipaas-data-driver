package com.pharbers.ipaas.data.driver.config.yamlModel

import scala.beans.BeanProperty

/** job配置实体，读取yaml或json生成，包含在operator配置中， name为类路径
  *
  * @author dcs
  */
class Plugin {
    @BeanProperty var name = ""
    @BeanProperty var factory = ""
}
