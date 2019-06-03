package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhBaseAction(operatorLst: List[PhOperatorTrait], name: String) extends PhActionTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = ???
}
