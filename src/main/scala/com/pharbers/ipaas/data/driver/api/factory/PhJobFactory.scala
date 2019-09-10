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

import com.pharbers.ipaas.data.driver.api.model.Job
import com.pharbers.ipaas.data.driver.exceptions.PhBuildJobException
import com.pharbers.ipaas.data.driver.api.work.{PhActionTrait, PhJobTrait, PhMapArgs, PhStringArgs, PhWorkArgs}

/** Job 实体工厂
 *
 * @param job model.Job 对象
 * @author dcs
 * @version 0.1
 * @since 2019/06/14 15:30
 */
case class PhJobFactory(job: Job)(implicit ctx: PhMapArgs[PhWorkArgs[_]]) extends PhFactoryTrait[PhJobTrait] {

    /** 构建 Job 运行实例 */
    override def inst(): PhJobTrait = {
        import scala.collection.JavaConverters._

        val args = job.getArgs match {
            case null => Map[String, PhStringArgs]().empty
            case one => one.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        }

        val actions = job.getActions match {
            case null => Seq()
            case lst => lst.asScala.map { action =>
                try {
                    getMethodMirror(action.getFactory)(action, ctx).asInstanceOf[PhFactoryTrait[PhActionTrait]].inst()
                } catch {
                    case e: PhBuildJobException => throw PhBuildJobException(e.configs ++ List(job.name + ":" + args.mkString(",")), e.exception)
                    case e: Exception => throw e
                }
            }
        }

        getMethodMirror(job.getReference)(
            job.getName,
            PhMapArgs(args),
            actions,
            ctx
        ).asInstanceOf[PhJobTrait]
    }
}
