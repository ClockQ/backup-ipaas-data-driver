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

package com.pharbers.ipaas.data.driver.plugins

import com.pharbers.ipaas.data.driver.api.Annotation.Plugin
import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

/** 去掉Seq[String]类型的最后一个元素，剩余元素拼成以指定分隔符分隔的String
 *
 * @author clock
 * @version 0.1
 * @since 2019/6/27 15:16
 * @example 默认参数例子
 * {{{
 *     colName: String 要进行操作的列
 *     delimiter: "," // 合并后字符串的分隔符，默认为空格
 * }}}
 */
@Plugin(name = "drop_seq_last", args = Array("colName", "delimiter"), msg = "Remove the last of the Seq and to string")
case class DropLastInSeqPlugin(name: String,
                               defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                               subPluginLst: Seq[PhPluginTrait[Column]])(implicit ctx: PhMapArgs[PhWorkArgs[_]])
        extends PhPluginTrait[Column] {
    /** 要转换的列名 */
    val colName: String = defaultArgs.getAs[PhStringArgs]("colName").get.get
    /** 分隔符 */
    val delimiter: String = defaultArgs.getAs[PhStringArgs]("delimiter") match {
        case Some(one) => one.get
        case None => " "
    }

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        val formatFunc: UserDefinedFunction = udf { lst: Seq[String] => lst.dropRight(1).mkString(delimiter) }
        PhColArgs(formatFunc(col(colName)))
    }
}
