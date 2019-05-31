package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work.{PhWorkArgs, PhWorkTrait}

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
trait PhFactoryTrait {
    def inst(args: PhWorkArgs[_]): PhWorkTrait
}
