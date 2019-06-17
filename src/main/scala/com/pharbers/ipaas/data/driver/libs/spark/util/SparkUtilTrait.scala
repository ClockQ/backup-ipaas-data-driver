package com.pharbers.ipaas.data.driver.libs.spark.util

import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/** SPARK 常用工具集
  *
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
trait SparkUtilTrait {
    /** SPARK 连接实例
      *
      * @author clock
      * @version 0.1
      * @since 2019/5/20 15:27
      * @note
      */
    implicit val conn_instance: SparkConnInstance
}