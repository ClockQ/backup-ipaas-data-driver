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
package com.pharbers.ipaas.data.driver

import com.pharbers.ipaas.data.driver.libs.read._
import com.pharbers.ipaas.data.driver.api.model.Job
import com.pharbers.ipaas.data.driver.libs.input.{JsonInput, YamlInput}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.api.factory.{PhFactoryTrait, getMethodMirror}
import com.pharbers.ipaas.data.driver.api.work.{PhJobTrait, PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}

object Main {

    def main(args: Array[String]): Unit = {
        if(args.length != 3) throw new Exception("args length is not equal to 3")

        val jobArgs = args(2)
        val readStream = args(1).toUpperCase() match {
            case "BUFFER" => StringRead(jobArgs).toInputStream()
            case "LOCAL" => LocalRead(jobArgs).toInputStream()
            case "HDFS" => HDFSRead(jobArgs).toInputStream()
            case "OSS" => OssRead(jobArgs).toInputStream()
        }
        val jobs = args(0).toUpperCase() match {
            case "YAL" | "YAML" => YamlInput().readObjects[Job](readStream)
            case "JSON" => JsonInput().readObjects[Job](readStream)
        }

        implicit val sd: PhSparkDriver = PhSparkDriver("job-context")
        sd.sc.setLogLevel("ERROR")
        val ctx = PhMapArgs(Map(
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
        ))
        val phJobs = jobs.map(x => getMethodMirror(x.getFactory)(x, ctx).asInstanceOf[PhFactoryTrait[PhJobTrait]].inst())
        phJobs.head.perform(PhMapArgs(Map()))
        println("执行完成，job_id:" + sd.sc.getConf.getAppId)
    }
}
