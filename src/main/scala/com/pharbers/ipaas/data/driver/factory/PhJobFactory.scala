package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel._
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException


/** 功能描述
  *
  * @param job 配置文件解析成的对象
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 16:11
  * @note 一些值得注意的地方
  */
case class PhJobFactory(job: Job) extends PhFactoryTrait[PhJobTrait] {
   /** 功能描述
     *
     * 构建Job实例
     * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhJobTrait
     * @throws PhOperatorException 用以捕获构建算子时的异常，及定位算子配置
     * @author EDZ
     * @version 0.0
     * @since 2019/6/11 16:11
     * @note 一些值得注意的地方
     * @example {{{这是一个例子}}}
     */
    override def inst(): PhJobTrait = {
        import scala.collection.JavaConverters._
        val actions = job.getActions.asScala.map(x => {
            try{
                PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhActionFactory].inst()
            } catch {
                case e: PhOperatorException => throw PhOperatorException(e.names :+ job.name, e.exception)
            }
        }).toList
        val tmp = PhFactory.getMethodMirror(job.getReference)(actions, job.getName)
        tmp.asInstanceOf[PhJobTrait]
    }
}
