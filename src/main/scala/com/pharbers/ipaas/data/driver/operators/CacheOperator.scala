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

package com.pharbers.ipaas.data.driver.operators

/** 缓存DataFrame，保证一些随机生成列的值不会变化
 *
 * @author dcs
 * @version 0.1
 * @since 2019/6/15 18:10
 * @example 默认参数例子
 * {{{
 *     inDFName: actionName // 要缓存的 DataFrame 名字
 * }}}
 */

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhOperatorTrait, PhPluginTrait, PhStringArgs, PhWorkArgs}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

case class CacheOperator(name: String,
						 defaultArgs: PhMapArgs[PhWorkArgs[Any]],
						 pluginLst: Seq[PhPluginTrait[Column]])
	extends PhOperatorTrait[DataFrame] {
	/** 要缓存的 DataFrame 名字 */
	val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
	/** 缓存等级 */
	val level: String = defaultArgs.getAs[PhStringArgs]("level").getOrElse(PhStringArgs("MEMORY_ONLY")).get
	override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
		val inDF = pr.getAs[PhDFArgs](inDFName).get.get
		val outDF = inDF.persist(StorageLevel.fromString(level))
		PhDFArgs(outDF)
	}
}
