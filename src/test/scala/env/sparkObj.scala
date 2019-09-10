package env

import com.pharbers.ipaas.data.driver.api.work.{PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.session.{SparkConnConfig, SparkConnInstance}
import com.pharbers.ipaas.data.driver.libs.spark.util.SparkUtilTrait
import com.pharbers.ipaas.data.driver.libs.spark.{PhSparkDriver, SparkRunConfig}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/** Spark Driver 实例
 *
 * @author clock
 * @version 0.1
 * @since 2019/08/27 14:38
 * @note 测试使用，随时删除
 */
@deprecated
object sparkObj {
    implicit lazy val sparkDriver = PhTestSparkDriver("test-driver")
//    sparkDriver.sc.setLogLevel("ERROR")

    val logDriver = PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID"))

    implicit val ctx = PhMapArgs(Map(
        "sparkDriver" -> PhSparkDriverArgs(sparkDriver),
        "logDriver" -> PhLogDriverArgs(logDriver)
    ))
}

case class PhTestSparkDriver(applicationName: String) extends SparkConnInstance {

    /** 设置 Spark 工具集
      *
      * @param helper 工具集实例
      * @tparam T <: SparkUtilTrait 工具集的子类
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:15
      * @example 默认参数例子
      *          {{{
      *           this.setUtil(readParquet()).readParquet("hdfs:///test")
      *          }}}
      */
    def setUtil[T <: SparkUtilTrait](helper: T): T = helper

    /** 添加 Spark 运行时 Jar
      *
      * @param jarPath Jar包路径
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:17
      * @example 默认参数例子
      *          {{{
      *           this.addJar("hdfs:///test.jar")
      *          }}}
      */
    def addJar(jarPath: String): PhSparkDriver = {
        sc.addJar(jarPath)
        this
    }

    /** 停止 Spark 驱动，关闭 Spark Context 连接
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:17
      * @example 默认参数例子
      *          {{{
      *           this.stopSpark()
      *          }}}
      */
    def stopSpark(): Unit = this.sc.stop()
}

trait SparkTestConnInstance {

    //    System.setProperty("HADOOP_USER_NAME","spark")

    /** SPARK 连接实例名
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:08
      */
    val applicationName: String

    /** SPARK 连接配置
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:08
      */
    val connConf: SparkTestConnConfig.type = SparkTestConnConfig

    private val conf = new SparkConf()
            //测试用
            .set("spark.yarn.jars", connConf.yarnJars)
            .set("spark.yarn.archive", connConf.yarnJars)
            .set("yarn.resourcemanager.hostname", connConf.yarnResourceHostname)
            .set("yarn.resourcemanager.address", connConf.yarnResourceAddress)
            .setAppName(applicationName)
            .setMaster("yarn")
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.sql.crossJoin.enabled", "true")
            .set("spark.yarn.dist.files", connConf.yarnDistFiles)
            .set("spark.executor.memory", connConf.executorMemory)
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

    /** SPARK Session
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:08
      */
    implicit val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    /** SPARK Context
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:09
      */
    implicit val sc: SparkContext = ss.sparkContext

    /** SPARK SQL Context
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:09
      */
    implicit val sqc: SQLContext = ss.sqlContext
}

object SparkTestConnConfig {
    //    val configPath: String = "pharbers_config/spark-config.xml"

    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"
    val yarnResourceHostname: String = "spark.master"
    val yarnResourceAddress: String = "spark.master:8032"
    val yarnDistFiles: String = "hdfs://spark.master:9000/config"
    val executorMemory: String = "2g"
}
