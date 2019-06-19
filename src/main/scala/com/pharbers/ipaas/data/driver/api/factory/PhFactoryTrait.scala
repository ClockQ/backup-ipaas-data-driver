package com.pharbers.ipaas.data.driver.api.factory

import com.pharbers.ipaas.data.driver.api.work._

/** iPaas Driver 运行实体工厂
  *
  * @author dcs
  * @version 0.1
  * @since 2019/06/14 15:26
  */
trait PhFactoryTrait[T <: PhWorkTrait2[_]] {
    /** 构建运行实例
      *
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkTrait[T]
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 16:30
      */
    def inst(): T
}