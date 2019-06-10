package com.pharbers.ipaas.data.driver.factory

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.config.yamlModel._

import scala.reflect.runtime.universe

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class PhJobFactory(job: Job) extends PhFactoryTrait[PhJobTrait] {
    /**  这个方法干啥的
      *   @param   args  参数说明.
      *   @param   T
      *   @return
      *   @throws  Exception
      *   @example
      *   @note
      *   @history
      */
    override def inst(): PhJobTrait = {
        import scala.collection.JavaConverters._
        val actions = job.getActions.asScala.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhActionFactory].inst()).toList
        val tmp = PhFactory.getMethodMirror(job.getReference)(actions, job.getName)
        tmp.asInstanceOf[PhJobTrait]
    }
}
