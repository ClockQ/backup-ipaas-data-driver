package com.pharbers.ipaas.data.driver.api.model

/** iPaas Driver Plugin 运行实体
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 10:00
  * @note
  */
case class Plugin() extends Model {
    /** Plugin 嵌套的 Plugins
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 11:14
      * @note
      */
    var sub: Plugin = _

    def getSub: Plugin = sub

    def setSub(sub: Plugin): Unit = this.sub = sub
}