package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work.{PhPluginTrait, PhStringArgs, PhWorkArgs}

import scala.reflect.runtime.universe

/** 构造函数无参的plugin工厂
  *
  * @author dcs
  * @note 通过反射获取
  */
case class PhPluginFactory() extends PhFactoryTrait {
    def inst(args: PhWorkArgs[_]): PhPluginTrait = {
        val mapArgs = args.toMapArgs[PhStringArgs].get.map(x => (x._1, x._2.get))

        val m = universe.runtimeMirror(getClass.getClassLoader)
        val classSy = m.classSymbol(Class.forName(mapArgs("name")))
        val cm = m.reflectClass(classSy)
        val ctor = classSy.toType.decl(universe.termNames.CONSTRUCTOR).asMethod
        val ctorm = cm.reflectConstructor(ctor)
        val tmp = ctorm()
        tmp.asInstanceOf[PhPluginTrait]
    }
}
