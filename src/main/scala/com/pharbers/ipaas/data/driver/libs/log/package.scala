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

/** work包实例
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/18 15:41
  * @note 常用 log 工具
  */
package object log {
    def formatMsg(user: String, traceID: String, jobID: String)(msgs: Seq[Any]): String = {
        s"""user: $user, traceID: $traceID, jobID: $jobID, ${msgs.map(_.toString).mkString(", ")}"""
    }
}
