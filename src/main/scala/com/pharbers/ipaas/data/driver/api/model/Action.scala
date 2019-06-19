package com.pharbers.ipaas.data.driver.api.model

/** iPaas Driver Action 运行实体
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 10:00
  * @note
  */
case class Action() extends Model {
    /** Action 包含的 Operators
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 11:30
      * @note
      */
    var opers: java.util.List[Operator] = _

    def getOpers: java.util.List[Operator] = opers

    def setOpers(opers: java.util.List[Operator]): Unit = this.opers = opers
}