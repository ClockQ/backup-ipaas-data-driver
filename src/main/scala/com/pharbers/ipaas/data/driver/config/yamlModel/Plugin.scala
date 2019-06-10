package com.pharbers.ipaas.data.driver.config.yamlModel

import java.util
import java.util.Map

import scala.beans.BeanProperty

/** job配置实体，读取yaml或json生成，包含在operator配置中， name为类路径
  *
  * @author dcs
  */
class Plugin {
    var name = ""
    var factory = ""
    var reference = ""
    var args: java.util.Map[String, String] = _

    def getName: String = name

    def setName(name: String): Unit = {
        this.name = name
    }

    def getFactory: String = factory

    def setFactory(factory: String): Unit = {
        this.factory = factory
    }

    def getReference: String = reference

    def setReference(reference: String): Unit = {
        this.reference = reference
    }

    def getArgs: util.Map[String, String] = args

    def setArgs(args: util.Map[String, String]): Unit = {
        this.args = args
    }
}
