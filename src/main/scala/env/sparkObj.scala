package env

import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/**
  * @description:
  * @author: clock
  * @date: 2019-05-30 13:50
  */
object sparkObj extends PhSparkDriver("testSparkObj"){
    sc.addJar("target/ipaas-data-driver-0.1.jar")
    sc.setLogLevel("ERROR")
}
