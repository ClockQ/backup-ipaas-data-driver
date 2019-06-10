package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhBaseJob(actionLst: List[PhActionTrait], name: String) extends PhJobTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        if (actionLst.isEmpty) pr
        else {
            try {
                val tmp = pr match {
                    case mapArgs: PhMapArgs[_] => PhMapArgs(mapArgs.get +
                            (actionLst.head.name -> actionLst.head.perform(pr)))
                    case _ => pr
                }

                PhBaseJob(actionLst.tail, name).perform(tmp)
            }catch {
                case e: PhOperatorException => {
                    phlog.setErrorLog("jobName#actionName#operatorName -> " + name + "#" + e.getMessage)
                    phlog.setErrorLog(e.getStackTrace.map(x => x.toString).mkString("\n"))
                    pr
                }
            }
        }
    }
}
