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

/** 读取输入文件接口
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/11 15:27
  * @note
  */
trait InputTrait {

    /** 读取为单个对象 T
      *
      * @param stream 配置文件输入流
      * @tparam T 返回的对象类型
      * @return _root_.scala.reflect.ClassTag[T] => T
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 15:36
      */
    def readObject[T: ClassTag](stream: InputStream): T

    /** 读取为对象集合 Seq[T]
      *
      * @param stream 配置文件输入流
      * @tparam T 返回的对象类型
      * @return _root_.scala.reflect.ClassTag[T] => scala.Seq[T]
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 15:36
      */
    def readObjects[T: ClassTag](stream: InputStream): Seq[T]
}
