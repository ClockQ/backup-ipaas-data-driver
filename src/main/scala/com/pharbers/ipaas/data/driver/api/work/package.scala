package com.pharbers.ipaas.data.driver.api

/**
  * @version V0.1
  * @author clock
  * @className work
  * @packageName com.pharbers.ipaas.data.driver.api
  * @description work包对象, 主要有implicit和转换函数
  * @data 2019-05-30 16:58
  **/
package object work {
    implicit class implicitOps(work: PhWorkArgs[_]) {
        def toMapArgs[A <: PhWorkArgs[_]]: PhMapArgs[A] = work.asInstanceOf[PhMapArgs[A]]
        def toListArgs: PhListArgs[_] = work.asInstanceOf[PhListArgs[_]]
        def toRDDArgs: PhRDDArgs[_] = work.asInstanceOf[PhRDDArgs[_]]
        def toDFArgs: PhDFArgs = work.asInstanceOf[PhDFArgs]
        def toColArgs: PhColArgs = work.asInstanceOf[PhColArgs]
    }
}
