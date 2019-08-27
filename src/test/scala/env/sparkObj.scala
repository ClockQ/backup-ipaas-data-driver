package env

import com.pharbers.ipaas.data.driver.api.work.{PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** Spark Driver 实例
 *
 * @author clock
 * @version 0.1
 * @since 2019/08/27 14:38
 * @note 测试使用，随时删除
 */
@deprecated
object sparkObj {
    val sparkDriver = PhSparkDriver("test-driver")
    sparkDriver.sc.setLogLevel("ERROR")

    val logDriver = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))

    implicit val ctx = PhMapArgs(Map(
        "sparkDriver" -> PhSparkDriverArgs(sparkDriver),
        "logDriver" -> PhLogDriverArgs(logDriver)
    ))
}
