package com.pharbers.ipaas.data.driver.api.factory

import com.pharbers.ipaas.data.driver.api.model.Action
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException
import com.pharbers.ipaas.data.driver.api.work.{PhActionTrait2, PhMapArgs, PhOperatorTrait2, PhStringArgs}

/** Action 实体工厂
  *
  * @param action model.Action 对象
  * @author dcs
  * @version 0.1
  * @since 2019/06/14 15:30
  */
case class PhActionFactory(action: Action) extends PhFactoryTrait[PhActionTrait2] {

    /** 构建 Action 运行实例 */
    override def inst(): PhActionTrait2 = {
        import scala.collection.JavaConverters._

        val args = action.getArgs match {
            case null => Map[String, PhStringArgs]().empty
            case one => one.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        }

        val opers = action.getOpers.asScala match {
            case null => Seq()
            case lst => lst.map { oper =>
                try {
                    getMethodMirror(oper.getFactory)(oper).asInstanceOf[PhFactoryTrait[PhOperatorTrait2[Any]]].inst()
                } catch {
                    case e: PhOperatorException => throw PhOperatorException(e.names :+ action.name, e.exception)
                }
            }
        }

        getMethodMirror(action.getReference)(
            action.getName,
            PhMapArgs(args),
            opers
        ).asInstanceOf[PhActionTrait2]
    }
}
