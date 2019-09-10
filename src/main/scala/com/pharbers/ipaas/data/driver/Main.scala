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
import com.pharbers.util.log.PhLogable._
import com.pharbers.ipaas.data.driver.api.factory.{PhFactoryTrait, getMethodMirror}
import com.pharbers.ipaas.data.driver.api.work.{PhJobTrait, PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs}
import com.pharbers.ipaas.data.driver.libs.kafka.ProducerAvroTopic
import com.pharbers.kafka.schema.ListeningJobTask

object Main {

    def main(args: Array[String]): Unit = {
	    // 第一波对接成功后整理Kafka的库封装
        val record = new ListeningJobTask()
	    try {
            if(args.length != 4) throw new Exception("args length is not equal to 4")
            val jobArgs = args(2)
            val readStream = args(1).toUpperCase() match {
                case "STRING" => StringRead(jobArgs).toInputStream()
                case "LOCAL" => LocalRead(jobArgs).toInputStream()
                case "HDFS" => HDFSRead(jobArgs).toInputStream()
                case "OSS" => OssRead(jobArgs).toInputStream()
            }
            val jobs = args(0).toUpperCase() match {
                case "YAL" | "YAML" => YamlInput().readObjects[Job](readStream)
                case "JSON" => JsonInput().readObjects[Job](readStream)
            }
        
            implicit val sd: PhSparkDriver = PhSparkDriver("job-context")

//            record.put("JobId", sd.sc.getConf.getAppId)
            record.put("JobId", args(3))
            record.put("Status", "Running")
            record.put("Message", "")
            record.put("Progress", "0")
		    println("Running")
            ProducerAvroTopic("listeningJobTaskTest", record)
        
            sd.sc.setLogLevel("ERROR")
            val ctx = PhMapArgs(Map(
                "sparkDriver" -> PhSparkDriverArgs(sd)
            ))
		    
            val phJobs = jobs.map(x =>{
                x.jobId = args(3)
                getMethodMirror(x.getFactory)(x, ctx).asInstanceOf[PhFactoryTrait[PhJobTrait]].inst()
            })
            useJobId("test_user", "test_trace", args(3))(phJobs.head){
                x => x.perform(PhMapArgs(Map()))
            }
            println("Finish")
            record.put("JobId", args(3))
            record.put("Status", "Finish")
            record.put("Message", "Finish")
            record.put("Progress", "100")
            ProducerAvroTopic("listeningJobTaskTest", record)
        } catch {
            case e: Exception =>
                record.put("JobId", args(3))
                record.put("Status", "Error")
                record.put("Message", e.getMessage)
                record.put("Progress", "-1")
                ProducerAvroTopic("listeningJobTaskTest", record)
        }

        println("执行结束")
    }
}
