package com.pharbers.ipaas.data.driver.api

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:46
  */
package object work {
    implicit class implicitOps(work: PhWorkArgs[_]) {
        def toMapArgs[A <: PhWorkArgs[_]]: PhMapArgs[A] = work.asInstanceOf[PhMapArgs[A]]
        def toListArgs: PhListArgs[_] = work.asInstanceOf[PhListArgs[_]]
        def toRDDArgs: PhRDDArgs[_] = work.asInstanceOf[PhRDDArgs[_]]
        def toDFArgs: PhDFArgs = work.asInstanceOf[PhDFArgs]
        def toColArgs: PhColArgs = work.asInstanceOf[PhColArgs]
    }
}
