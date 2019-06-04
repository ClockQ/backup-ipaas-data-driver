package com.pharbers.ipaas.data.driver.job

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.DataFrame

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class PhBaseAction(operatorLst: List[PhOperatorTrait], name: String, args: PhWorkArgs[_]) extends PhActionTrait{
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        if (operatorLst.isEmpty) pr
        else {
            val tmp = pr match {
                case mapArgs: PhMapArgs[_] => mapArgs
                case _ => throw new Exception("参数类型错误")
            }
            val dfKey = args.toMapArgs[PhStringArgs].getAs[PhStringArgs]("df")
            val df = dfKey match {
                case key: Some[PhStringArgs] => tmp.get(key.get.get).asInstanceOf[PhWorkArgs[_]]
                case _ => PhNoneArgs
            }

            operatorLst.foldLeft(df)((left, right) => right.perform(PhMapArgs(tmp.get + ("df" -> left))))
        }
    }
}
