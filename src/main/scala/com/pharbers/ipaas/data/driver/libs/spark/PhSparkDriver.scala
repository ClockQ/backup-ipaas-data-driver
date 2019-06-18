/*
 * This file is part of com.pharbers.ipaas-data-driver.
 *
 * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.pharbers.ipaas.data.driver.libs.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import com.pharbers.ipaas.data.driver.libs.spark.util.SparkUtilTrait
import com.pharbers.ipaas.data.driver.libs.spark.session.SparkConnInstance

/** SPARK 驱动类
  *
  * @param applicationName SPARK 连接实例名
  * @author clock
  * @version 0.1
  * @since 2019/5/20 15:27
  * @note
  */
case class PhSparkDriver(applicationName: String) extends SparkConnInstance {
    /** SPARK 运行配置
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/17 11:08
      */
    val runConf: SparkRunConfig.type = SparkRunConfig

    if (runConf.jarsPath.startsWith("hdfs:///"))
        FileSystem.get(sc.hadoopConfiguration)
                .listStatus(new Path(runConf.jarsPath))
                .map(_.getPath.toString)
                .foreach(addJar)

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