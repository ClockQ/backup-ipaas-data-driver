package com.pharbers.ipaas.data.driver.funcs

import org.apache.spark.sql.Column
import com.pharbers.ipaas.data.driver.api.work.PhWorkArgs

/**
  * @description:
  * @author: clock
  * @date: 2019-05-29 16:38
  */
trait PhPluginArgs[+A] extends PhWorkArgs[A]

trait PhOperatorArgs[A] extends PhWorkArgs[A] {
    def perform(pr: A): A
}

final case class PhColArgs(args: Column) extends PhPluginArgs[Column]
