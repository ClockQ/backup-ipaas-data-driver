package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.PhWorkArgs

trait PhOperatorsTrait {
    val name: String
    val defaultArgs:  PhWorkArgs[_]
    def perform(args:  PhWorkArgs[_]):  PhWorkArgs[_]
}
