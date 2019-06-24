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

package com.pharbers.ipaas.data.driver.api.model

/** iPaas Driver 运行实体
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 9:06
  * @note
  */
trait Model {
    /** iPaas Driver 运行实体名字
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var name: String = _

    /** iPaas Driver 实例化工厂类签名
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var factory: String = _

    /** iPaas Driver 实例化目标类签名
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var reference: String = _

    /** iPaas Driver 实例化目标类参数
      *
      * @author clock
      * @version 0.1
      * @since 2019/06/14 9:06
      * @note
      */
    var args: java.util.Map[String, String] = _

    def getName: String = name

    def setName(name: String): Unit = this.name = name

    def getFactory: String = factory

    def setFactory(factory: String): Unit = this.factory = factory

    def getReference: String = reference

    def setReference(reference: String): Unit = this.reference = reference

    def getArgs: java.util.Map[String, String] = args

    def setArgs(args: java.util.Map[String, String]): Unit = this.args = args
}
