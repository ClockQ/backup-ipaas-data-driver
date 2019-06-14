package com.pharbers.ipaas.data.driver.api

import scala.reflect.runtime.universe

/** iPaas Driver 运行实体工厂
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 15:26
  */
package object factory {

    /** 反射获取类构造方法
      *
      * @param reference 类路径
      * @return _root_.scala.reflect.runtime.universe.MethodMirror
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 16:08
      */
    def getMethodMirror(reference: String): universe.MethodMirror = {
        val m = universe.runtimeMirror(getClass.getClassLoader)
        val classSy = m.classSymbol(Class.forName(reference))
        val cm = m.reflectClass(classSy)
        val ctor = classSy.toType.decl(universe.termNames.CONSTRUCTOR).asMethod
        cm.reflectConstructor(ctor)
    }
}
