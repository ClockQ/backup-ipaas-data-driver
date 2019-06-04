package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.config.yamlModel.PluginBean

/** 构造函数无参的plugin工厂
  *
  * @author dcs
  * @note 通过反射获取
  */
case class PhPluginFactory(plugin: PluginBean) extends PhFactoryTrait[PhPluginTrait] {
    override def inst(): PhPluginTrait = {
        import scala.collection.JavaConverters._
        val tmp = PhFactory.getMethodMirror(plugin.getReference)(plugin.getName, PhMapArgs(
            if(plugin.getArgs == null) Map()
            else plugin.getArgs.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        ))
        tmp.asInstanceOf[PhPluginTrait]
    }
}
