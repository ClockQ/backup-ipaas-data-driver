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

package com.pharbers.ipaas

import java.io.{File, FileInputStream}
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

import com.aliyun.oss.{OSS, OSSClientBuilder}
import com.aliyun.oss.model.OSSObject
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{JsonMappingException, ObjectMapper}
import com.pharbers.ipaas.data.driver.api.factory.{PhFactoryTrait, getMethodMirror}
import com.pharbers.ipaas.data.driver.api.model.Job
import com.pharbers.ipaas.data.driver.api.model.driverConfig.DriverConfig
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.input.{JsonInput, YamlInput}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogDriver, formatMsg}
import com.pharbers.ipaas.data.driver.libs.spark.{PhSparkDriver, util}
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.SparkJob
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.reflect.ClassTag

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/30 16:17
  * @note 一些值得注意的地方
  */
object Main {

    def main(args: Array[String]): Unit = {
        Runner.run(args)
    }
}

object Runner {
    val logger = PhLogDriver(formatMsg("driver_manager", "", ""))
    var jobs: List[Job] = List()
    val lock = new ReentrantLock(true)
    val aCondition: Condition = lock.newCondition

    def run(args: Array[String]): Unit = {
        val configPath = args.head
        val driverConfig = JsonInput().readObject[DriverConfig](new FileInputStream(new File(configPath)))
        implicit val sd: PhSparkDriver = PhSparkDriver(driverConfig.name)
        sd.sc.setLogLevel("error")
        logger.setInfoLog("创建成功", s"driver name: ${driverConfig.name}")
        kafkaListener(driverConfig.topic, sd)
    }

    def kafkaListener(topic: String, driver: PhSparkDriver): Unit = {
        val pkc = new PharbersKafkaConsumer[String, SparkJob](List(topic), 1000, Int.MaxValue, process)
        val t = new Thread(pkc)
        try {
            logger.setInfoLog("DriverListener starting!")
            t.start()
            var job: Job = null
            while (!driver.sc.isStopped) {
                //todo: 单独def
                lock.lock()
                while (jobs.isEmpty) aCondition.await()
                job = jobs.head
                jobs = jobs.tail
                lock.unlock()
                logger.setInfoLog("开始job",s"name:${job.name}")
                val phJob = getMethodMirror(job.getFactory)(job).asInstanceOf[PhFactoryTrait[PhJobTrait]].inst()
                phJob.perform(PhMapArgs(Map(
                    "sparkDriver" -> PhSparkDriverArgs(driver),
                    "logDriver" -> PhLogDriverArgs(PhLogDriver(formatMsg("test_user", "test_traceID", "test_jobID")))
                )))
                //todo: job完成判断， recall job结果
                logger.setInfoLog("job完成",s"name:${job.name}")
            }
        } catch {
            case ie: InterruptedException =>
                logger.setErrorLog(ie.getMessage)
        } finally {
            pkc.close()
            logger.setInfoLog("close!")
        }

    }

    def process(record: ConsumerRecord[String, SparkJob]): Unit = {
        //todo: 配置化
        val endpoint = "oss-cn-beijing.aliyuncs.com"
        val accessKeyId = "LTAIEoXgk4DOHDGi"
        val accessKeySecret = "x75sK6191dPGiu9wBMtKE6YcBBh8EI"
        try {
            val client: OSS = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret)
            val ossObj: OSSObject = client.getObject(record.value().getBucketName.toString, record.value().getOssKey.toString)
            val job = JsonInput().readObjects[Job](ossObj.getObjectContent)
            Runner.lock.lock()
            Runner.jobs = Runner.jobs ++ job
            Runner.aCondition.signalAll()
            Runner.lock.unlock()

        } catch {
            case e: JsonMappingException => logger.setErrorLog(e)
            case e: JsonParseException => logger.setErrorLog(e)
        }
    }
}
