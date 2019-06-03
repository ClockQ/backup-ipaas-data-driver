package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work.{PhNoneArgs, PhOperatorTrait, PhPluginTrait, PhWorkArgs}

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhBaseOperator(plugin: PhPluginTrait, name: String, args: PhWorkArgs[_]) extends PhOperatorTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = ???
}
