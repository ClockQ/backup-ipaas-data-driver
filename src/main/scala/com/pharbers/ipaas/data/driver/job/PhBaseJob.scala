package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhBaseJob(actionLst: List[PhActionTrait], name: String) extends PhJobTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//        if (actionLst.isEmpty) pr
//        else {
//            val tmp = pr match {
//                case mapArgs: PhMapArgs[_] => PhMapArgs(mapArgs.get +
//                        (actionLst.head.name -> actionLst.head.perform(pr)))
//                case _ => pr
//            }
//
//            PhBaseJob(actionLst.tail, name).perform(tmp)
//        }
        ???
    }
}
