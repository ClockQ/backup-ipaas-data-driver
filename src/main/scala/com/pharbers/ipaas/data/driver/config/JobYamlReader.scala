package com.pharbers.ipaas.data.driver.config

import java.io.InputStream

import com.pharbers.ipaas.data.driver.config.yamlModel._
import org.yaml.snakeyaml.{TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor

import scala.reflect.ClassTag

/** 读取job Yaml配置文件
  *
  * @author dcs
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

    /**  读取job yaml
      *   @param   yamlStream   yaml 流.
      *   @return   List[Job]
      *   @throws  Exception
      *   @example 这是一个例子
      *   @note yaml与类模型不符合是会报第一行不能解析
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

    /**  读取简单的yaml， 只能读取没有实例集合的配置
      *   @param  source   yaml 流.
      *   @param   T  输出实例的类型
      *   @return   实例
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
