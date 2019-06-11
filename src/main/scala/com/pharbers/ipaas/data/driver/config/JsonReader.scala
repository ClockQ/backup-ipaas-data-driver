package com.pharbers.ipaas.data.driver.config

import org.codehaus.jackson.map.ObjectMapper
import scala.reflect.ClassTag
import scala.tools.nsc.interpreter.InputStream

/** 功能描述
  *读取json配置文件
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 15:27
  * @note
  */
case class JsonReader() extends ConfigReaderTrait{
    /**  读取为List[T]
      *   @param   json   Json流.
      *   @param   T 持久化类型
      *   @return Seq[T]
      *   @throws  Exception
      *   @example readObjects[JobBean]
      *   @note 只能读取成List[T]
      *   @history
      */
    def readObjects[T:ClassTag](json: InputStream): Seq[T] ={
        import scala.collection.JavaConverters._
        val mapper = new ObjectMapper()
        val javaType2 = mapper.getTypeFactory.constructParametricType(classOf[java.util.List[T]], implicitly[ClassTag[T]].runtimeClass)
//        val ref = mapper.readValue(json, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]
        val ref = mapper.readValue(json, javaType2).asInstanceOf[java.util.List[T]]
        ref.asScala
    }

    /**  读取为单个对象
      *   @param   json   Json流.
      *   @param   T  持久化类型
      *   @return T
      *   @throws  Exception
      *   @example
      *   @note T 不能为集合
      *   @history
      */
    def readObject[T:ClassTag](json: InputStream): T ={
        val mapper = new ObjectMapper()
        val ref = mapper.readValue(json, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]
        ref
    }

}
