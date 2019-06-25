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

package com.pharbers.ipaas.data.driver.exceptions

/** 捕获job构建异常job->action->operator->plugin链
  *
  * @param configs operator /action /job name + args
  * @author EDZ
  * @version 0.1
  * @since 2019/06/24 11:50
  */
case class PhBuildJobException(configs: Seq[String], exception: Throwable) extends Exception {
    /** 获取异常描述
      *
      * @return _root_.scala.Predef.String
      * @author EDZ
      * @version 0.1
      * @since 2019/6/11 15:55
      * @note 一些值得注意的地方
      */
    override def getMessage: String = {
        configs.mkString("\n") + (List("\n" + exception.toString) ++ exception.getStackTrace.map(x => x.toString)).mkString("\n")
    }
}
