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

package com.pharbers.ipaas.data.driver.api.work

sealed trait PhWorkTrait extends Serializable {
    val name: String
    val defaultArgs: PhWorkArgs[_]

    def perform(pr: PhWorkArgs[_]): PhWorkArgs[_]
}

trait PhPluginTrait extends PhWorkTrait

trait PhOperatorTrait extends PhWorkTrait

/** CMD 运行实体基类
  *
  * @tparam A 运行实体泛型
  * @author clock
  * @version 0.1
  * @since 2019/6/11 16:43
  */
sealed trait PhWorkTrait2[+A] extends Serializable {
    /** 运行实例的名字
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    val name: String

    /** 运行实例的默认参数
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    val defaultArgs: PhMapArgs[PhWorkArgs[Any]]

    /** CMD执行方法
      *
      * @param pr 包含的子的 CMD 执行结果及之前执行过的 Action 中的结果
      * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkArgs[A]
      * @author clock
      * @version 0.1
      * @since 2019/6/15 15:24
      * @note
      * {{{
      *     pr中需要传递 `key` 为 `sparkDriver` 的 PhSparkDriverArgs
      *     pr中需要传递 `key` 为 `logDriver` 的 PhLogDriverArgs
      * }}}
      */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[A]
}

/** Plugin 运行实体基类
  *
  * @tparam A 运行实体泛型
  * @author clock
  * @version 0.1
  * @since 2019/6/11 16:43
  */
trait PhPluginTrait2[+A] extends PhWorkTrait2[A] {
    /** Plugin 实例中包含的子 Plugin
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    val subPluginLst: Seq[PhPluginTrait2[Any]]
}

/** Operator 运行实体基类
  *
  * @tparam A 运行实体泛型
  * @author clock
  * @version 0.1
  * @since 2019/6/11 16:43
  */
trait PhOperatorTrait2[+A] extends PhWorkTrait2[A] {
    /** Operator 实例中包含的 Plugin
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    val pluginLst: Seq[PhPluginTrait2[Any]]
}

/** Action 运行实体基类
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/11 16:43
  */
trait PhActionTrait extends PhWorkTrait2[Any] {
    /** Action 实例中包含的 Operator
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    val operatorLst: Seq[PhOperatorTrait2[Any]]
}

/** Job 运行实体基类
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/11 16:43
  */
trait PhJobTrait extends PhWorkTrait2[Any] {
    /** Job 实例中包含的 Action
      *
      * @author clock
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    val actionLst: Seq[PhActionTrait]
}