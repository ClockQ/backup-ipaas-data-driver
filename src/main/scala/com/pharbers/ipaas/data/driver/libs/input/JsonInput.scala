/*
 * This file is part of com.pharbers.ipaas-data-driver.
 *
 * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
 */

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
    //todo: 全局应该只有一个ObjectMapper
        JsonInput.mapper.readValue(stream, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]

    def readObject[T: ClassTag](json: String): T =
    //todo: 全局应该只有一个ObjectMapper
        JsonInput.mapper.readValue(json, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]

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
        val javaType = JsonInput.mapper.getTypeFactory.constructParametricType(classOf[java.util.List[T]], implicitly[ClassTag[T]].runtimeClass)
        JsonInput.mapper.readValue(stream, javaType).asInstanceOf[java.util.List[T]].asScala
    }
}

object JsonInput{
    val mapper =  new ObjectMapper()
}
