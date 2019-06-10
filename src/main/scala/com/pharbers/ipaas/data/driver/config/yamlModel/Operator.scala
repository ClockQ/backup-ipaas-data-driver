package com.pharbers.ipaas.data.driver.config.yamlModel

import java.util
import java.util.Map

import scala.beans.BeanProperty

/** job配置实体，读取yaml或json生成，包含在action配置中
  *
  * @author dcs
  */
case class Operator() {
    var name = ""
    var factory = ""
    var reference = ""
    var args: java.util.Map[String, String] = _
    var plugin: Plugin = _

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

    def getReference: String = reference

    def setReference(reference: String): Unit = {
        this.reference = reference
    }

    def getPlugin: Plugin = plugin

    def setPlugin(plugin: Plugin): Unit = {
        this.plugin = plugin
    }
}
