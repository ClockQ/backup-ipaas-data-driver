package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel._
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException

import scala.reflect.runtime.universe

/** 功能描述
  * Operator工厂
  * @param operator 配置文件解析成的对象
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 16:11
  * @note 一些值得注意的地方
  */
case class PhOperatorFactory(operator: Operator) extends PhFactoryTrait[PhOperatorTrait] {

    /** 功能描述
      *
     构建算子实例
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhOperatorTrait
      * @throws PhOperatorException 构建算子时的异常
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 16:20
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    override def inst(): PhOperatorTrait = {
        import scala.collection.JavaConverters._
        val tmp = try{
            val plugin = operator.getPlugin match {
                case _: Plugin => PhFactory.getMethodMirror(operator.getPlugin.getFactory)(operator.getPlugin).asInstanceOf[PhFactoryTrait[PhPluginTrait]].inst()
                case _ => null
            }
            PhFactory.getMethodMirror(operator.getReference)(plugin, operator.getName, PhMapArgs(operator.getArgs.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap))
        } catch {
            case e: Exception => throw PhOperatorException(List(operator.name), e)
        }
        tmp.asInstanceOf[PhOperatorTrait]
    }
}
