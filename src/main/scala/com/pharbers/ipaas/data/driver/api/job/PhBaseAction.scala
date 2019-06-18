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
import com.pharbers.ipaas.data.driver.exceptions.PhOperatorException

/** Action 运行实体
  *
  * @param name        Action 名字
  * @param defaultArgs 配置参数
  * @param operatorLst Action 包含的 Operator 列表
  * @author dcs
  * @version 0.1
  * @since 2019/06/11 15:30
  */
case class PhBaseAction(name: String,
                        defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                        operatorLst: Seq[PhOperatorTrait2[Any]])
        extends PhActionTrait {

    /** Action 执行入口
      *
      * @param pr operator 运行时储存的结果
      * @throws PhOperatorException operator执行时异常
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 16:43
      */
    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
        if (operatorLst.isEmpty) pr
        else {
            operatorLst.foldLeft(pr) { (l, r) =>
                try {
                    PhMapArgs(l.get + (r.name -> r.perform(l)))
                } catch {
                    case e: Exception => throw PhOperatorException(List(r.name, name), e)
                }

            }.get(operatorLst.last.name)
        }
    }
}
