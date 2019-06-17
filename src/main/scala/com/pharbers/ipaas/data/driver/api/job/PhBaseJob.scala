package com.pharbers.ipaas.data.driver.api.job

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException
import com.pharbers.ipaas.data.driver.log.Phlog

/** Job 运行实体
  *
  * @param name        Job 名字
  * @param defaultArgs 配置参数
  * @param actionLst   Job 包含的 Action 列表
  * @author dcs
  * @version 0.1
  * @since 2019/6/11 16:50
  */
case class PhBaseJob(name: String,
                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                     actionLst: Seq[PhActionTrait]) extends PhJobTrait {

    /** Job 执行入口
      *
      * @param pr action 运行时储存的结果
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
        if (actionLst.isEmpty) pr
        else {
            actionLst.foldLeft(pr) { (l, r) =>
                try {
                    PhMapArgs(l.get + (r.name -> r.perform(l)))
                } catch {
                    case e: PhOperatorException =>
                        Phlog().setErrorLog(PhOperatorException(e.names :+ name, e.exception).getMessage)
                        pr
                }
            }
        }
    }
}
