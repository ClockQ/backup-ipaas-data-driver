package com.pharbers.ipaas.data.driver.libs.input

import scala.reflect.ClassTag
import org.codehaus.jackson.map.ObjectMapper
import scala.tools.nsc.interpreter.InputStream

/** 读取json配置文件
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/11 15:27
  */
case class JsonInput() extends InputTrait {

    /** 读取Json数据为单个对象 T
      *
      * @example
      * {{{
      * val data =
      * """
      * |{
      * |    "name" : "testName",
      * |    "factory" : "testFactory"
      * |}
      * """.stripMargin
      * val stream = new ByteArrayInputStream(data.getBytes)
      * val plugin = JsonInput().readObject[Plugin](stream)
      * }}}
      */
    def readObject[T: ClassTag](stream: InputStream): T =
        new ObjectMapper().readValue(stream, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]

    /** 读取Json数据为对象集合 Seq[T]
      *
      * @example
      * {{{
      * val data =
      * """
      * |[
      * |    {
      * |        "name" : "testName1",
      * |        "factory" : "testFactory1"
      * |    },
      * |    {
      * |        "name" : "testName2",
      * |        "factory" : "testFactory2"
      * |    }
      * |]
      * """.stripMargin
      * val stream = new ByteArrayInputStream(data.getBytes)
      * val plugin = JsonInput().readObjects[Plugin](stream)
      * }}}
      */
    def readObjects[T: ClassTag](stream: InputStream): Seq[T] = {
        import scala.collection.JavaConverters._
        val mapper = new ObjectMapper()
        val javaType = mapper.getTypeFactory.constructParametricType(classOf[java.util.List[T]], implicitly[ClassTag[T]].runtimeClass)
        mapper.readValue(stream, javaType).asInstanceOf[java.util.List[T]].asScala
    }
}
