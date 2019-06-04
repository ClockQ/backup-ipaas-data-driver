package com.pharbers.ipaas.data.driver.api.work

import com.pharbers.ipaas.data.driver.log.{PhLogMsg, Phlog}

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 15:52
  */
sealed trait PhWorkTrait extends Serializable {
	val name: String
	val defaultArgs: PhWorkArgs[_]

	def perform(pr: PhWorkArgs[_]): PhWorkArgs[_]

	val phlog = Phlog()
	phlog.setInfoLog(PhLogMsg("user", "traceID", "jobID", this.getClass.toString.split("\\.").last, "description").toString)
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