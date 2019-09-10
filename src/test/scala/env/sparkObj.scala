package env

import com.pharbers.ipaas.data.driver.api.work.{PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Spark Driver 实例
 *
 * @author clock
 * @version 0.1
 * @since 2019/08/27 14:38
 * @note 测试使用，随时删除
 */
@deprecated
object sparkObj {
    implicit lazy val sparkDriver: PhTestSparkDriver = new PhTestSparkDriver("test-driver")
//    sparkDriver.sc.setLogLevel("ERROR")

    val logDriver = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))

    implicit val ctx = PhMapArgs(Map(
        "sparkDriver" -> PhSparkDriverArgs(sparkDriver),
        "logDriver" -> PhLogDriverArgs(logDriver)
    ))
}

class PhTestSparkDriver(applicationTestName: String) extends PhSparkDriver(applicationTestName) {
    private val conf = new SparkConf()
            //测试用
            .set("spark.yarn.jars", SparkTestConnConfig.yarnJars)
            .set("spark.yarn.archive", SparkTestConnConfig.yarnJars)
            .set("yarn.resourcemanager.hostname", SparkTestConnConfig.yarnResourceHostname)
            .set("yarn.resourcemanager.address", SparkTestConnConfig.yarnResourceAddress)
            .setAppName(applicationName)
            .setMaster("yarn")
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.sql.crossJoin.enabled", "true")
            .set("spark.yarn.dist.files", SparkTestConnConfig.yarnDistFiles)
            .set("spark.executor.memory", SparkTestConnConfig.executorMemory)
            .set("spark.driver.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,adress=5005")
            .set("spark.executor.extraJavaOptions",
                """
                  | -XX:+UseG1GC -XX:+PrintFlagsFinal
                  | -XX:+PrintReferenceGC -verbose:gc
                  | -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
                  | -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions
                  | -XX:+G1SummarizeConcMark
                  | -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=1
                """.stripMargin)
    //            .set("spark.sql.shuffle.partitions", "4")
    //            .set("spark.sql.cbo.enabled", "true")
    override  implicit val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

}

object SparkTestConnConfig{
    //    val configPath: String = "pharbers_config/spark-config.xml"

    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"
    val yarnResourceHostname: String = "spark.master"
    val yarnResourceAddress: String = "spark.master:8032"
    val yarnDistFiles: String = "hdfs://spark.master:9000/config"
    val executorMemory: String = "2g"
}
