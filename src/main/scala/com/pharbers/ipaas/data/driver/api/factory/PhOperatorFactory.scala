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

package com.pharbers.ipaas.data.driver.api.factory

import java.lang.reflect.InvocationTargetException

import com.pharbers.ipaas.data.driver.api.model.{Operator, Plugin}
import com.pharbers.ipaas.data.driver.exceptions.{PhBuildJobException, PhOperatorException}
import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs}

/** Operator实体工厂
  *
  * @param operator model.Operator 对象
  * @author dcs
  * @version 0.1
  * @since 2019/06/14 15:30
  */
case class PhOperatorFactory(operator: Operator) extends PhFactoryTrait[PhOperatorTrait[Any]] {

    /** 构建 Operator 运行实例
      *
      * @throws com.pharbers.ipaas.data.driver.exceptions.PhOperatorException 构建算子时的异常
      * */
    override def inst(): PhOperatorTrait[Any] = {
        import scala.collection.JavaConverters.mapAsScalaMapConverter

        val args = operator.getArgs match {
            case null => Map[String, PhStringArgs]().empty
            case one => one.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        }


        val plugin = operator.getPlugin match {
            case null => Seq()
            case one: Plugin => try {
                Seq(getMethodMirror(one.getFactory)(one).asInstanceOf[PhFactoryTrait[PhPluginTrait[Any]]].inst())
            }catch {
                case e: PhBuildJobException =>
                    throw PhBuildJobException(e.configs ++ List(operator.name + ":" + operator.args.asScala.mkString(",")), e.exception)
                case e: Exception => throw e
            }
        }
        try {
            getMethodMirror(operator.getReference)(
                operator.getName,
                PhMapArgs(args),
                plugin
            ).asInstanceOf[PhOperatorTrait[Any]]
        } catch {
            case e: InvocationTargetException => throw PhBuildJobException(List(operator.name + ":" + operator.args.asScala.mkString("\n")), e.getCause)
            case e: Exception => throw e
        }
    }
}
