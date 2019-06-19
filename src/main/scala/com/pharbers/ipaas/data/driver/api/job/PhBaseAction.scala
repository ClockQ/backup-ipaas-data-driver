package com.pharbers.ipaas.data.driver.api.job

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException

/** Action 运行实体
  *
  * @param name        Action 名字
  * @param defaultArgs 配置参数
  * @param operatorLst Action 包含的 Operator 列表
  * @author dcs
  * @version 0.1
  * @since 2019/06/11 15:30
  */
case class PhBaseAction(name: String,
                        defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                        operatorLst: Seq[PhOperatorTrait2[Any]])
        extends PhActionTrait {

    /** Action 执行入口
      *
      * @param pr operator 运行时储存的结果
      * @throws PhOperatorException operator执行时异常
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
        if (operatorLst.isEmpty) pr
        else {
            operatorLst.foldLeft(pr) { (l, r) =>
                try {
                    PhMapArgs(l.get + (r.name -> r.perform(l)))
                } catch {
                    case e: Exception => throw PhOperatorException(List(r.name, name), e)
                }

            }.get(operatorLst.last.name)
        }
    }
}
