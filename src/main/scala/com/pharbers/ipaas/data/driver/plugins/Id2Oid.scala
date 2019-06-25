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

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/** id to oid 插件
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/24 15:16
  * @example df.CalcRingGrowth("$name", CalcMat().CalcRankByWindow(PhMapArgs).get)
  * {{{
  *      idColName: String id在列名
  * }}}
  */
case class Id2Oid(name: String,
                  defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                  subPluginLst: Seq[PhPluginTrait[Column]])
	extends PhPluginTrait[Column] {
//	id所在列名
	val idColName: String = defaultArgs.getAs[PhStringArgs]("idColName").get.get
    val trimOIdUdf: UserDefinedFunction = udf(oidSchema)

	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(trimOIdUdf(col(idColName)))
    }
}

/** oidSchema
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/24 15:16
  * {{{
  *      idColName: String oid
  * }}}
  */
private[plugins] case class oidSchema(oid: String) {
    val oidSchema = StructType(StructField("oid", StringType, nullable = false) :: Nil)
    new GenericRowWithSchema(Array(oid), oidSchema)
}