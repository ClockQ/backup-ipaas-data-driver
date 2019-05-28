package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.PhWorkArgs

case class CalcRank extends PhOperatorsTrait{
    override val name: String = _
    override val defaultArgs: PhWorkArgs[_] = _

    override def perform(args: PhWorkArgs): PhWorkArgs = ???
}
