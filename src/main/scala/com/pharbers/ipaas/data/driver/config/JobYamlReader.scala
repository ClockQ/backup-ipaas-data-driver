package com.pharbers.ipaas.data.driver.config

import java.io.InputStream

import com.pharbers.ipaas.data.driver.config.yamlModel._
import org.yaml.snakeyaml.{TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor

import scala.reflect.ClassTag

/** 这个类是干啥的
  *
  * @author dcs
  * @param args 参数说明
  * @tparam T 类型参数说明
  * @note 一些值得注意的地方
  */
case class JobYamlReader(){
    val constructor = new Constructor(classOf[Job])
    val carDescriptionJob = new TypeDescription(classOf[Job])
    carDescriptionJob.putListPropertyType("actions", classOf[Action])

    val carDescriptionAction = new TypeDescription(classOf[Action])
    carDescriptionAction.putMapPropertyType("args", classOf[String], classOf[String])
    carDescriptionAction.putListPropertyType("opers", classOf[Operator])

    val carDescriptionOpers = new TypeDescription(classOf[Operator])
    carDescriptionOpers.putMapPropertyType("args", classOf[String], classOf[String])
    carDescriptionOpers.putListPropertyType("plugins", classOf[Plugin])

    val carDescriptionPlugin = new TypeDescription(classOf[Plugin])

    constructor.addTypeDescription(carDescriptionJob)
    constructor.addTypeDescription(carDescriptionAction)
    constructor.addTypeDescription(carDescriptionOpers)
    constructor.addTypeDescription(carDescriptionPlugin)
    val yaml = new Yaml(constructor)

    /**  这个方法干啥的
      *   @param   yamlStream   参数说明.
      *   @return   返回值类型及说明
      *   @throws  Exception 异常类型及说明
      *   @example 这是一个例子
      *   @note 一些值得注意的地方
      *   @history 记录修改历史，暂时在这儿写最后一个修改的是谁
      */
    def read(yamlStream: InputStream): List[Job] = {

        val iterator = yaml.loadAll(yamlStream).iterator()
        var result: List[Job] = Nil
        while (iterator.hasNext){
            result = result :+ iterator.next().asInstanceOf[Job]
        }
        result
    }

    /**  这个方法干啥的
      *   @param  source   参数说明.
      *   @param   T  类型参数说明
      *   @return   返回值类型及说明
      *   @throws  Exception 异常类型及说明
      *   @example 这是一个例子
      *   @note 不能使用在有转换后有集合的情况
      *   @history 记录修改历史，暂时在这儿写最后一个修改的是谁
      */
    def loadConfig[T:ClassTag](source: InputStream): T = {
        val constructor = new Constructor(implicitly[ClassTag[T]].runtimeClass)
        val yaml = new Yaml(constructor)
        yaml.load(source).asInstanceOf[T]
    }
}
