package com.pharbers.ipaas.data.driver.funcs

import org.apache.spark.sql.Column
import com.pharbers.ipaas.data.driver.api.work.PhWorkArgs

/**
  * @description:
  * @author: clock
  * @date: 2019-05-29 16:38
  */
trait PhPluginArgs[+A] extends PhWorkArgs[A] {
    val args: A
    def get: A = args
}

trait PhOperatorArgs[A] extends PhWorkArgs[A] {
    def perform(pr: A): A
}

