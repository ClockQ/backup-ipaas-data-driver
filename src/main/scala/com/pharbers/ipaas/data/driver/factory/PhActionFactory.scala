package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel.Action
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException

import scala.reflect.runtime.universe

/** Action工厂
  *
  * @author dcs
  * @param action 持久化的配置对象
  * @note 一些值得注意的地方
  */
case class PhActionFactory(action: Action) extends PhFactoryTrait[PhActionTrait] {

 /** 功能描述
   *
   * 构建Action实例
   * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhActionTrait
   * @throws PhOperatorException 用以捕获构建算子时的异常，及定位算子配置
   * @author EDZ
   * @version 0.0
   * @since 2019/6/11 16:05
   * @note 一些值得注意的地方
   * @example {{{这是一个例子}}}
   */
    override def inst(): PhActionTrait = {
        import scala.collection.JavaConverters._
        val operators = action.getOpers.asScala.map(x => {
            try{
                PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhOperatorFactory].inst()
            }catch {
                case e:PhOperatorException => throw PhOperatorException(e.names :+ action.name, e.exception)
            }
        }).toList
        val tmp = PhFactory.getMethodMirror(action.getReference)(operators, action.getName, PhMapArgs(action.getArgs.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap))
        tmp.asInstanceOf[PhActionTrait]
    }
}
