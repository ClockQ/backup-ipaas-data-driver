package com.pharbers.ipaas.data.driver.api

import scala.reflect.ClassTag

/** work包实例
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/13 17:28
  * @note
  */
package object work {

    /** PhWorkArgs 可用的隐式类
      *
      * @param args PhWorkArgs类型的参数
      * @tparam T PhWorkArgs类型中包装的参数类型
      * @author clock
      * @version 0.1
      * @since 2019/6/13 17:31
      * @note
      */
    implicit class implicitOps[T: ClassTag](work: PhWorkArgs[T]) {
        /** 将 PhWorkArgs[T] 强制转为 PhMapArgs[A]
          *
          * @tparam A Map中value的类型参数
          * @return PhMapArgs[A]
          * @author clock
          * @version 0.1
          * @since 2019/6/13 17:33
          */
        def toMapArgs[A <: PhWorkArgs[_]]: PhMapArgs[A] = work.asInstanceOf[PhMapArgs[A]]

        /** 将 PhWorkArgs[T] 强制转为 PhListArgs[_]
          *
          * @return PhListArgs[_]
          * @author clock
          * @version 0.1
          * @since 2019/6/13 17:34
          */
        def toListArgs: PhListArgs[_] = work.asInstanceOf[PhListArgs[_]]

        /** 将 PhWorkArgs[T] 强制转为 PhRDDArgs[_]
          *
          * @return PhRDDArgs[_]
          * @author clock
          * @version 0.1
          * @since 2019/6/13 17:34
          */
        def toRDDArgs: PhRDDArgs[_] = work.asInstanceOf[PhRDDArgs[_]]

        /** 将 PhWorkArgs[T] 强制转为 PhDFArgs
          *
          * @return PhDFArgs
          * @author clock
          * @version 0.1
          * @since 2019/6/13 17:34
          */
        def toDFArgs: PhDFArgs = work.asInstanceOf[PhDFArgs]

        /** 将 PhWorkArgs[T] 强制转为 PhColArgs
          *
          * @return PhDFArgs
          * @author clock
          * @version 0.1
          * @since 2019/6/13 17:34
          */
        def toColArgs: PhColArgs = work.asInstanceOf[PhColArgs]
    }

}
