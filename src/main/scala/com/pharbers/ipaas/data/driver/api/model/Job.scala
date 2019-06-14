package com.pharbers.ipaas.data.driver.api.model

/** iPaas Driver Action 运行实体
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 10:00
  * @note
  */
case class Job() extends Model {
    /** Job 包含的 Actions
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 11:30
      * @note
      */
    var actions: java.util.List[Action] = _

    def getActions: java.util.List[Action] = actions

    def setActions(actions: java.util.List[Action]): Unit = this.actions = actions
}