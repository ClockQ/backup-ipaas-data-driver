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

/** 全局常量定义
 *
 * @author clock
 * @version 0.1
 * @since 2019/06/25 16:17
 */
object PhSysConsts {
    val LOCALHOST_NAME: String = java.net.InetAddress.getLocalHost.getHostName

//    val SPARK_CONF_DIR: String = sys.env("PH_DRIVER_SPARK_CONF")
//
//    val TEST_CONF_DIR: String = sys.env("PH_DRIVER_TEST_CONF")
}
