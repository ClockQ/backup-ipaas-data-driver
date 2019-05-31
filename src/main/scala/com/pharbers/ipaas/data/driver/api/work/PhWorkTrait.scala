package com.pharbers.ipaas.data.driver.api.work

import com.pharbers.ipaas.data.driver.log.Phlog

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 15:52
  */
sealed trait PhWorkTrait extends Serializable with Phlog{
    val name: String
    val defaultArgs: PhWorkArgs[_]
    def perform(pr: PhWorkArgs[_]): PhWorkArgs[_]
    setInfoLog("start****" + this.getClass.toString)
}

trait PhPluginTrait extends PhWorkTrait

trait PhPluginTrait2[+A]

trait PhOperatorTrait extends PhWorkTrait

trait PhActionTrait extends PhWorkTrait {
    val operatorLst: List[PhOperatorTrait]
}

trait PhJobTrait extends PhWorkTrait {
    val actionLst: List[PhActionTrait]
}