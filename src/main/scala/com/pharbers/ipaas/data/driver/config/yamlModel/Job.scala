package com.pharbers.ipaas.data.driver.config.yamlModel

import java.util
import java.util.List

import com.pharbers.ipaas.data.driver.api.work.PhStringArgs

import scala.beans.BeanProperty

/** job配置实体，读取yaml或json生成
  *
  * @author dcs
  */
class Job(){
    var name = ""
    var factory = ""
    var reference = ""
    var actions: java.util.List[Action] = _

    def getName: String = name

    def setName(name: String): Unit = {
        this.name = name
    }

    def getFactory: String = factory

    def setFactory(factory: String): Unit = {
        this.factory = factory
    }

    def getActions: util.List[Action] = actions

    def setActions(actions: util.List[Action]): Unit = {
        this.actions = actions
    }

    def getReference: String = reference

    def setReference(reference: String): Unit = {
        this.reference = reference
    }
}
