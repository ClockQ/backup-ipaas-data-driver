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

package com.pharbers.ipaas.data.driver.libs

import com.pharbers.ipaas.data.driver.api.work.{PhListArgs, PhStringArgs}

/** work包实例
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/18 15:41
  * @note 常用 log 工具
  */
package object log {
    implicit val string2Ph: String => PhStringArgs = x => PhStringArgs(x)
    implicit val ph2String: PhStringArgs => String = x => x.get
    implicit val list2Ph: Seq[String] => PhListArgs[PhStringArgs] = x => PhListArgs(x.map(string2Ph).toList)
    implicit val ph2List: PhListArgs[PhStringArgs] => Seq[String] = x => x.get.map(x => x.get)
    implicit val string2List: String => PhListArgs[PhStringArgs] = x =>  PhListArgs(List(x))

    def formatMsg(user: String, traceID: String, jobID: String)(msgs: Seq[Any]): String = {
        s""""Hostname": "${PhConstant.LOCALHOST_NAME}","UserId": "$user","TraceID": "$traceID","JobID": "$jobID","Message": "${msgs.map(x => if(x != null) x.toString).mkString(", ")}"""".stripMargin
    }
}
