package com.pharbers.ipaas.data.driver.config.yamlModel

import java.util
import java.util.{List, Map}

import scala.beans.BeanProperty

/** action配置实体，读取yaml或json生成,包含在job配置中
  *
  * @author dcs
  */
class Action() {
    var name = ""
    var factory = ""
    var reference = ""
    var args: java.util.Map[java.lang.String, java.lang.String] = _
    var opers: java.util.List[Operator] = _

    def getName: String = name

    def setName(name: String): Unit = {
        this.name = name
    }

    def getFactory: String = factory

    def setFactory(factory: String): Unit = {
        this.factory = factory
    }

    def getArgs: util.Map[String, String] = args

    def setArgs(args: util.Map[String, String]): Unit = {
        this.args = args
    }

    def getOpers: util.List[Operator] = opers

    def setOpers(opers: util.List[Operator]): Unit = {
        this.opers = opers
    }

    def getReference: String = reference

    def setReference(reference: String): Unit = {
        this.reference = reference
    }
}
