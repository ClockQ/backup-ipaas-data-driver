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
