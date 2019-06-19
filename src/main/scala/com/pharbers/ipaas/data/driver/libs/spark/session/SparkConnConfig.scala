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

package com.pharbers.ipaas.data.driver.libs.spark.session

private[spark] object SparkConnConfig {
//    val configPath: String = "pharbers_config/spark-config.xml"

    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"
    val yarnResourceHostname: String = "spark.master"
    val yarnResourceAddress: String = "spark.master:8032"
    val yarnDistFiles: String = "hdfs://spark.master:9000/config"
    val executorMemory: String = "2g"
}