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

package com.pharbers.ipaas.data.driver.api.job

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.log.PhLogDriver
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** Job 运行实体
 *
 * @param name        Job 名字
 * @param defaultArgs 配置参数
 * @param actionLst   Job 包含的 Action 列表
 * @author dcs
 * @version 0.1
 * @since 2019/6/11 16:50
 */
case class PhBaseJob(name: String,
                     defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                     actionLst: Seq[PhActionTrait])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhJobTrait {

    val _: PhSparkDriver = ctx.get("sparkDriver").asInstanceOf[PhSparkDriverArgs].get
    val log: PhLogDriver = ctx.get("logDriver").asInstanceOf[PhLogDriverArgs].get

    /** Job 执行入口
     *
     * @param pr action 运行时储存的结果
     * @author dcs
     * @version 0.1
     * @since 2019/6/11 16:43
     */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
        if (actionLst.isEmpty) pr
        else {
            actionLst.foldLeft(pr) { (l, r) =>
                try {
                    log.setInfoLog(r.name, "开始执行")
                    PhMapArgs(l.get + (r.name -> r.perform(l)))
                } catch {
                    case e: PhOperatorException =>
                        log.setErrorLog(PhOperatorException(e.names :+ name, e.exception).getMessage)
                        throw e
                }
            }
        }
    }
}
