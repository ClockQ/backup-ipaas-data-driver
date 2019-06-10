package com.pharbers.ipaas.data.driver.api

import scala.reflect.ClassTag

/**
  * @version V0.1
  * @author clock
  * @className work
  * @packageName com.pharbers.ipaas.data.driver.api
  * @description work包对象, 主要有implicit和转换函数
  * @data 2019-05-30 16:58
  **/
package object work {
    implicit class implicitOps[T: ClassTag](work: PhWorkArgs[T]) {
        @deprecated
        def toMapArgs[A <: PhWorkArgs[_]]: PhMapArgs[A] = work.asInstanceOf[PhMapArgs[A]]

        @deprecated
        def toListArgs: PhListArgs[_] = work.asInstanceOf[PhListArgs[_]]

        @deprecated
        def toRDDArgs: PhRDDArgs[_] = work.asInstanceOf[PhRDDArgs[_]]

        @deprecated
        def toDFArgs: PhDFArgs = work.asInstanceOf[PhDFArgs]

        @deprecated
        def toColArgs: PhColArgs = work.asInstanceOf[PhColArgs]
    }
}
