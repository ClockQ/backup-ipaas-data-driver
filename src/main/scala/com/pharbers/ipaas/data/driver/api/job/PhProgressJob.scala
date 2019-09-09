package com.pharbers.ipaas.data.driver.api.job

import java.time.Duration

import com.pharbers.ipaas.data.driver.api.work.{PhActionTrait, PhJobTrait, PhLogDriverArgs, PhMapArgs, PhSparkDriverArgs, PhStringArgs, PhWorkArgs}
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException
import com.pharbers.ipaas.data.driver.libs.kafka.ProducerAvroTopic
import com.pharbers.ipaas.data.driver.libs.log.PhLogDriver
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{JobRequest, ListeningJobTask}
import org.apache.avro.specific.SpecificRecord

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/09/05 19:45
  * @note 一些值得注意的地方
  */
case class PhProgressJob(name: String,
                         defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                         actionLst: Seq[PhActionTrait])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhJobTrait {
    val _: PhSparkDriver = ctx.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get
    val log: PhLogDriver = ctx.get("logDriver").asInstanceOf[PhLogDriverArgs].get
    val topic: String = defaultArgs.getAs[PhStringArgs]("topic").getOrElse(PhStringArgs("listeningJobTaskTest")).get
    val jobId: String = defaultArgs.getAs[PhStringArgs]("jobId").getOrElse(PhStringArgs(" ")).get
    /** Job 执行入口
      *
      * @param pr action 运行时储存的结果
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] =  {
        val pkp = new PharbersKafkaProducer[String, ListeningJobTask]
        if (actionLst.isEmpty) pr
        else {
            try {
                actionLst.zipWithIndex.foldLeft(pr) { (l, right) =>
                    try {
                        val action = right._1
                        log.setInfoLog(action.name, "开始执行")
                        pkp.produce(topic, jobId, new ListeningJobTask(jobId, "Running", "", (100 * right._2 / actionLst.length).toString))
                        PhMapArgs(l.get + (action.name -> action.perform(l)))
                    } catch {
                        case e: PhOperatorException =>
                            log.setErrorLog(PhOperatorException(e.names :+ name, e.exception).getMessage)
                            throw e
                    }
                }
            }finally {
                pkp.producer.close(Duration.ofSeconds(10))
            }
        }
    }
}
