package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work.{PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.config.yamlModel.PluginBean

import scala.reflect.runtime.universe

/** 构造函数无参的plugin工厂
  *
  * @author dcs
  * @note 通过反射获取
  */
case class PhPluginFactory(plugin: PluginBean) extends PhFactoryTrait[PhPluginTrait] {
    override def inst(): PhPluginTrait = {
        val tmp = PhFactory.getRef(plugin.getName)()
        tmp.asInstanceOf[PhPluginTrait]
    }
}
