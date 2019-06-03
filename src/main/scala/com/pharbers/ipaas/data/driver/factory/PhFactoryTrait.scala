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
    def getRef(reference: String): MethodMirror ={
        val m = universe.runtimeMirror(getClass.getClassLoader)
        val classSy = m.classSymbol(Class.forName(reference))
        val cm = m.reflectClass(classSy)
        val ctor = classSy.toType.decl(universe.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
    }
}