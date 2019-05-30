package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.functions._

/**
  *
  * @param args PhMapArgs
  * @tparam T
  */
class addByWhen[T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) extends PhOperatorTrait{
    override val name: String = "add column by when"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val condition = args.get.getOrElse("condition",throw new Exception("not found condition")).get.asInstanceOf[String]
        val trueValue = args.get.getOrElse("trueValue",throw new Exception("not found trueValue")).get
        val otherValue = args.get.getOrElse("otherValue",throw new Exception("not found otherValue")).get

        PhColArgs(when(expr(condition), trueValue).otherwise(otherValue))
    }
}
