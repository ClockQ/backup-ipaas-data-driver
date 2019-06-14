package env

import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** Spark Driver 实例
  *
  * @author clock
  * @version 0.1
  * @since 2019-05-30 13:50
  * @note 测试使用，随时删除
  */
@deprecated
object sparkObj extends PhSparkDriver("testSparkObj"){
    sc.addJar("target/ipaas-data-driver-0.1.jar")
    sc.setLogLevel("ERROR")
}
