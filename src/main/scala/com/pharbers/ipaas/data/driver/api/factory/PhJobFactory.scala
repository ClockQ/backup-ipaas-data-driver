package com.pharbers.ipaas.data.driver.api.factory

import com.pharbers.ipaas.data.driver.api.model.Job
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException
import com.pharbers.ipaas.data.driver.api.work.{PhActionTrait2, PhJobTrait2, PhMapArgs, PhStringArgs}

/** Job 实体工厂
  *
  * @param action model.Job 对象
  * @author dcs
  * @version 0.1
  * @since 2019/06/14 15:30
  */
case class PhJobFactory(job: Job) extends PhFactoryTrait[PhJobTrait2] {

    /** 构建 Job 运行实例 */
    override def inst(): PhJobTrait2 = {
        import scala.collection.JavaConverters._

        val args = job.getArgs match {
            case null => Map[String, PhStringArgs]().empty
            case one => one.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        }

        val actions = job.getActions.asScala match {
            case null => Seq()
            case lst => lst.map { action =>
                try {
                    getMethodMirror(action.getFactory)(action).asInstanceOf[PhFactoryTrait[PhActionTrait2]].inst()
                } catch {
                    case e: PhOperatorException => throw PhOperatorException(e.names :+ job.name, e.exception)
                }
            }
        }

        getMethodMirror(job.getReference)(
            job.getName,
            PhMapArgs(args),
            actions
        ).asInstanceOf[PhJobTrait2]
    }
}
