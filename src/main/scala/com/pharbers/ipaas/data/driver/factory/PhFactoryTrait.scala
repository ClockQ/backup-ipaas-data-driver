package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work.{PhWorkArgs, PhWorkTrait}

/** factory trait
  *
  * @author dcs
  */
trait PhFactoryTrait {
    def inst(args: PhWorkArgs[_]): PhWorkTrait
}
