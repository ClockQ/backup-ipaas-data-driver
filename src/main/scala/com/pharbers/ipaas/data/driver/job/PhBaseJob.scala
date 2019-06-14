package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException

/** 功能描述
  * job基类
  *
  * @param actionLst action集合
  * @param name      job name
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 16:50
  * @note 一些值得注意的地方
  */
case class PhBaseJob(name: String,
                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                     actionLst: Seq[PhActionTrait2]) extends PhJobTrait2 {

    /** 功能描述
      * job运行入口
      *
      * @param pr 运行时储存action的结果
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkArgs[_]
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 16:50
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
        if (actionLst.isEmpty) pr
        else {
            try {
                val tmp = pr match {
                    case mapArgs: PhMapArgs[PhWorkArgs[Any]] => PhMapArgs(mapArgs.get +
                            (actionLst.head.name -> actionLst.head.perform(pr)))
                    case _ => pr
                }

                PhBaseJob(name, defaultArgs, actionLst.tail).perform(tmp)
            } catch {
                case e: PhOperatorException => {
//                    phlog.setErrorLog(PhOperatorException(e.names :+ name, e.exception).getMessage)
                    pr
                }
            }
        }
    }
}
