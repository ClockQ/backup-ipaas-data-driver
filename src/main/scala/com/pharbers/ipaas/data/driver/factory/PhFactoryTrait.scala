package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/** factory trait
  *
  * @author dcs
  */
trait PhFactoryTrait[T <: PhWorkTrait] {
    def inst(): T
}

object PhFactory{
    /** 功能描述
      *
     反射获取类构造方法
      * @param reference 类路径
      * @return _root_.scala.reflect.runtime.universe.MethodMirror
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 16:08
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    def getMethodMirror(reference: String): MethodMirror ={
        val m = universe.runtimeMirror(getClass.getClassLoader)
        val classSy = m.classSymbol(Class.forName(reference))
        val cm = m.reflectClass(classSy)
        val ctor = classSy.toType.decl(universe.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
    }
}