package com.pharbers.ipaas.data.driver.libs.spark.util

import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/**
  * Created by clock on 18-2-27.
  */
trait SparkUtilTrait {
    implicit val conn_instance: SparkConnInstance
}