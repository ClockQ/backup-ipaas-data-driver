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

import com.pharbers.ipaas.data.driver.api.model.Action
import com.pharbers.ipaas.data.driver.exceptions.PhBuildJobException
import com.pharbers.ipaas.data.driver.api.work.{PhActionTrait, PhMapArgs, PhOperatorTrait, PhStringArgs, PhWorkArgs}

/** Action 实体工厂
 *
 * @param action model.Action 对象
 * @author dcs
 * @version 0.1
 * @since 2019/06/14 15:30
 */
case class PhActionFactory(action: Action)(ctx: PhMapArgs[PhWorkArgs[_]]) extends PhFactoryTrait[PhActionTrait] {

    /** 构建 Action 运行实例 */
    override def inst(): PhActionTrait = {
        import scala.collection.JavaConverters._

        val args = action.getArgs match {
            case null => Map[String, PhStringArgs]().empty
            case one => one.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        }

        val opers = action.getOpers.asScala match {
            case null => Seq()
            case lst => lst.map { oper =>
                try {
                    getMethodMirror(oper.getFactory)(oper, ctx).asInstanceOf[PhFactoryTrait[PhOperatorTrait[Any]]].inst()
                } catch {
                    case e: PhBuildJobException => throw PhBuildJobException(e.configs ++ List(action.name + ":" + args.map(x => (x._1, x._2.get)).mkString(",")), e.exception)
                    case e: Exception => throw e
                }
            }
        }

        getMethodMirror(action.getReference)(
            action.getName,
            PhMapArgs(args),
            opers,
            ctx
        ).asInstanceOf[PhActionTrait]
    }
}
