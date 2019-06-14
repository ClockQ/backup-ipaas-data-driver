package com.pharbers.ipaas.data.driver.api.model

/** iPaas Driver 运行实体
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 9:06
  * @note
  */
trait Model {
    /** iPaas Driver 运行实体名字
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var name: String = _

    /** iPaas Driver 实例化工厂类签名
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var factory: String = _

    /** iPaas Driver 实例化目标类签名
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var reference: String = _

    /** iPaas Driver 实例化目标类参数
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var args: java.util.Map[String, String] = _

    def getName: String = name

    def setName(name: String): Unit = this.name = name

    def getFactory: String = factory

    def setFactory(factory: String): Unit = this.factory = factory

    def getReference: String = reference

    def setReference(reference: String): Unit = this.reference = reference

    def getArgs: java.util.Map[String, String] = args

    def setArgs(args: java.util.Map[String, String]): Unit = this.args = args
}
