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

package com.pharbers.ipaas.data.driver.libs.log

import org.apache.logging.log4j.{LogManager, Logger}

/** log4j2 驱动
  *
  * @author cui
  * @version 0.1
  * @since 2019-06-4 10:21
  * @note 改为实现PhLogable接口
  */
@Deprecated
case class PhLogDriver(formatMsg: Seq[Any] => String = _.map(_.toString).mkString(", ")) {
    private val logger: Logger = LogManager.getRootLogger()

    /** 输出 Trace 级别日志，来跟踪过程
     *
     * @param msg 日志内容
     * @return Unit
     * @author cui
     * @version 0.1
     * @since 2019-06-4 10:21
     * @note 显示在Debug模式下，应用与程序的跟踪过程，仅仅是一个程序调试跟踪，格式直接打印系统消息。调试程序时追踪程序走势。
     */
    def setTraceLog(msg: Any*): Unit = {
        logger.trace(formatMsg(msg))
    }

    /** 输出 Debug 级别日志，来验证数值
     *
     * @param msg 日志内容
     * @return Unit
     * @author cui
     * @version 0.1
     * @since 2019-06-4 10:21
     * @note 显示在Debug模式下，应用的调试。打印在程序流程过程中的一个具体数据的值。如果是监控程序流，请用TRACE。
     */
    def setDebugLog(msg: Any*): Unit = {
        logger.debug(formatMsg(msg))
    }

    /** 输出 Info 级别日志，来记录行为
     *
     * @param msg 日志内容
     * @return Unit
     * @author cui
     * @version 0.1
     * @since 2019-06-4 10:21
     * @note 显示在Product模式下，展示系统在运行过程中，必须要知道的细节。用于用户Log收集、Log分析、Log监控等。
     */
    def setInfoLog(msg: Any*): Unit = {
        logger.info(formatMsg(msg))
    }

    /** 输出 Warn 级别日志，来统计警告
     *
     * @param msg 日志内容
     * @return Unit
     * @author cui
     * @version 0.1
     * @since 2019-06-4 10:21
     * @note 显示在Product模式下，展示系统在运行过程中，产生警告的运行程序。用于监控系统中产生资源警告的信息。
     */
    def setWarnLog(msg: Any*): Unit = {
        logger.warn(formatMsg(msg))
    }

    /** 输出 Error 级别日志，来统计可捕获错误
     *
     * @param msg 日志内容
     * @return Unit
     * @author cui
     * @version 0.1
     * @since 2019-06-4 10:21
     * @note 显示Product模式下的可捕获错误。
     */
    def setErrorLog(msg: Any*): Unit = {
        logger.error(formatMsg(msg))
    }

    /** 输出 Fatal 级别日志，来统计严重错误
     *
     * @param msg 日志内容
     * @return Unit
     * @author cui
     * @version 0.1
     * @since 2019-06-4 10:21
     * @note 显示Product模式下的严重错误。
     */
    def setFatalLog(msg: Any*): Unit = {
        logger.fatal(formatMsg(msg))
    }
}
