package com.pharbers.ipaas.data.driver.config

import com.pharbers.ipaas.data.driver.config.yamlModel.Job
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper

import scala.reflect.ClassTag
import scala.tools.nsc.interpreter.InputStream

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
case class JobJsonReader() {
    /**  读取为List[T]
      *   @param   json   参数说明.
      *   @param   T
      *   @return
      *   @throws  Exception
      *   @example
      *   @note 只能读取成List[T]
      *   @history
      */
    def readObjects[T:ClassTag](json: InputStream): java.util.List[T] ={
        val mapper = new ObjectMapper()
        val javaType2 = mapper.getTypeFactory.constructParametricType(classOf[java.util.List[T]], implicitly[ClassTag[T]].runtimeClass)
//        val ref = mapper.readValue(json, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]
        val ref = mapper.readValue(json, javaType2).asInstanceOf[java.util.List[T]]
        ref
    }

    /**  读取为单个对象
      *   @param   json   参数说明.
      *   @param   T
      *   @return
      *   @throws  Exception
      *   @example
      *   @note
      *   @history
      */
    def readObject[T:ClassTag](json: InputStream): T ={
        val mapper = new ObjectMapper()
        val ref = mapper.readValue(json, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]
        ref
    }
}

case class test(name: String, factory: String)