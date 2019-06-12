package com.pharbers.ipaas.data.driver.config

import java.io.InputStream
import com.pharbers.ipaas.data.driver.config.yamlModel._
import org.yaml.snakeyaml.{TypeDescription, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import scala.reflect.ClassTag

/** 功能描述
  *读取yaml配置文件
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 15:27
  * @note
  */
case class YamlReader() extends ConfigReaderTrait{

    /**  读取yaml 为Seq[Object]
      *   @param   yamlStream   yaml 流.
      *   @return   List[Job]
      *   @example 这是一个例子
      *   @note yaml与类模型不符合是会报第一行不能解析
      *   @history 记录修改历史，暂时在这儿写最后一个修改的是谁
      */
    override def readObjects[T:ClassTag] (yamlStream: InputStream): Seq[T] = {

        val iterator = new Yaml(new Constructor(implicitly[ClassTag[T]].runtimeClass)).loadAll(yamlStream).iterator()
        var result: List[T] = Nil
        while (iterator.hasNext){
            result = result :+ iterator.next().asInstanceOf[T]
        }
        result
    }

    /**  读取简单的yaml
      *   @param  source   yaml 流.
      *   @param   T  输出实例的类型
      *   @return   T
      *   @example 这是一个例子
      *   @note
      *   @history 记录修改历史，暂时在这儿写最后一个修改的是谁
      */
    override def readObject[T:ClassTag](source: InputStream): T = {
        val constructor = new Constructor(implicitly[ClassTag[T]].runtimeClass)
        val yaml = new Yaml(constructor)
        yaml.load(source).asInstanceOf[T]
    }


}
