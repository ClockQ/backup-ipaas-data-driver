package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException
import org.apache.spark.sql.DataFrame

/** 功能描述
  * action基类
  *
  * @param operatorLst 算子
  * @param name        action name
  * @param args        配置参数
  * @author dcs
  * @version 0.0
  * @since ${YEAR}/${MONTH}/${DAY} ${TIME}
  * @note 一些值得注意的地方
  */
case class PhBaseAction(name: String,
                        defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                        operatorLst: Seq[PhOperatorTrait2[Any]])
        extends PhActionTrait2 {

    /** 功能描述
      * action执行入口
      *
      * @param pr 运行时其他action的结果 PhMapArgs[PhDfArgs]
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkArgs[_]
      * @throws PhOperatorException 算子执行时异常
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 16:43
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
        if (operatorLst.isEmpty) pr
        else {
            val dfKey = defaultArgs.toMapArgs[PhStringArgs].getAs[PhStringArgs]("df")
            val df = dfKey match {
                case key: Some[PhStringArgs] => pr.get(key.get.get)
                case _ => PhNoneArgs
            }

            operatorLst.foldLeft(df)((left, right) => {
                try {
                    right.perform(PhMapArgs(pr.get + ("df" -> left)))
                } catch {
                    case e: Exception => throw PhOperatorException(List(right.name, name), e)
                }
            })

        }
    }
}
