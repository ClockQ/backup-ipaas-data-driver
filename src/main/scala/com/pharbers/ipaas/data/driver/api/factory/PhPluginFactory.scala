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

import com.pharbers.ipaas.data.driver.api.model.Plugin
import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhStringArgs, PhPluginTrait2}

/** Plugin实体工厂
  *
  * @param plugin model.Plugin 对象
  * @author dcs
  * @version 0.1
  * @since 2019/06/14 15:30
  */
case class PhPluginFactory(plugin: Plugin) extends PhFactoryTrait[PhPluginTrait2[Any]] {

    /** 构建 Plugin 运行实例 */
    override def inst(): PhPluginTrait2[Any] = {
        import scala.collection.JavaConverters.mapAsScalaMapConverter

        val args = plugin.getArgs match {
            case null => Map[String, PhStringArgs]().empty
            case one => one.asScala.map(x => (x._1, PhStringArgs(x._2))).toMap
        }

        val sub = plugin.getSub match {
            case null => Seq()
            case one: Plugin => Seq(getMethodMirror(one.getFactory)(one).asInstanceOf[PhFactoryTrait[PhPluginTrait2[Any]]].inst())
        }

        getMethodMirror(plugin.getReference)(
            plugin.getName,
            PhMapArgs(args),
            sub
        ).asInstanceOf[PhPluginTrait2[Any]]
    }
}
