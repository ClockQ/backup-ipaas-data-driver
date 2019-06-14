package com.pharbers.ipaas.data.driver.api.model

/** iPaas Driver Operator 运行实体
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 10:00
  * @note
  */
case class Operator() extends Model {
    /** Operator 包含的 Plugins
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 11:14
      * @note
      */
    var plugin: Plugin = _

    def getPlugin: Plugin = plugin

    def setPlugin(plugin: Plugin): Unit = this.plugin = plugin
}