package com.pharbers.ipaas.data.driver.libs.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import com.pharbers.ipaas.data.driver.libs.spark.util.SparkUtilTrait
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/**
  * Created by clock on 18-2-26.
  */
case class PhSparkDriver(applicationName: String) extends SparkConnInstance {
    val runConf: SparkRunConfig.type = SparkRunConfig

    if (runConf.jarsPath.startsWith("hdfs:///"))
        FileSystem.get(sc.hadoopConfiguration)
                .listStatus(new Path(runConf.jarsPath))
                .map(_.getPath.toString)
                .foreach(addJar)

    def setUtil[T <: SparkUtilTrait](helper: T): T = helper

    def addJar(jarPath: String): PhSparkDriver = {
        sc.addJar(jarPath)
        this
    }

    def stopSpark(): Unit = this.sc.stop()
}