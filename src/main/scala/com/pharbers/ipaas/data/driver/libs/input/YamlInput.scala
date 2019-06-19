package com.pharbers.ipaas.data.driver.libs.input

import java.io.InputStream
import scala.reflect.ClassTag
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/** 读取Yaml配置文件
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/11 15:37
  */
case class YamlInput() extends InputTrait {

    /** 读取Yaml数据为单个对象 T
      *
      * @example
      * {{{
      * val data =
      * """
      * |name: testName
      * |factory: testFactory
      * |subs:
      * |  - name: subTestName
      * |    factory: testFactory
      * """.stripMargin
      * val stream = new ByteArrayInputStream(data.getBytes)
      * val plugin = YamlInput().readObject[Plugin](stream)
      * }}}
      */
    override def readObject[T: ClassTag](stream: InputStream): T = {
        val constructor = new Constructor(implicitly[ClassTag[T]].runtimeClass)
        new Yaml(constructor).load(stream).asInstanceOf[T]
    }

    /** 读取Yaml数据为对象集合 Seq[T]
      *
      * @example
      * {{{
      * val data =
      * """
      * |name: testName
      * |factory: testFactory
      * |sub:
      * |  name: subTestName
      * |  factory: testFactory
      * """.stripMargin
      * val stream = new ByteArrayInputStream(data.getBytes)
      * val plugin = YamlInput().readObject[Plugin](stream)
      * }}}
      */
    override def readObjects[T: ClassTag](stream: InputStream): Seq[T] = {
        val iterator = new Yaml(new Constructor(implicitly[ClassTag[T]].runtimeClass)).loadAll(stream).iterator()
        var result: List[T] = Nil
        while (iterator.hasNext) {
            result = result :+ iterator.next().asInstanceOf[T]
        }
        result
    }
}
