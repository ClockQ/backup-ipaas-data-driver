package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel._

/** 功能描述
  * Plugin工厂
  * @param plugin 配置文件解析成的对象
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 16:11
  * @note 一些值得注意的地方
  */
case class PhPluginFactory(plugin: Plugin) extends PhFactoryTrait[PhPluginTrait] {

    /** 功能描述
      *
     构建Plugin
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhPluginTrait
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 16:30
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    override def inst(): PhPluginTrait = {
        import scala.collection.JavaConverters._
        val tmp = PhFactory.getMethodMirror(plugin.getReference)(plugin.getName, PhMapArgs(
            if(plugin.getArgs == null) Map()
            else plugin.getArgs.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        ))
        tmp.asInstanceOf[PhPluginTrait]
    }
}
