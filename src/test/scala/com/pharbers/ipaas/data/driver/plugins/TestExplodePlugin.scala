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

import org.apache.spark.sql.DataFrame
import com.pharbers.ipaas.data.driver.api.work._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.operators.AddColumnOperator

class TestExplodePlugin extends FunSuite with BeforeAndAfterAll {
	implicit var sd: PhSparkDriver = _
	var testDF: DataFrame = _

	override def beforeAll(): Unit = {
		sd = PhSparkDriver("test-driver")
		val tmp = sd.ss.implicits
		import tmp._

		testDF = List(
			("name1", "prod1", "201801", "1#2#3"),
			("name2", "prod2", "201801", "1#2#3"),
			("name3", "prod1", "201802", "1#2#3"),
			("name4", "prod2", "201802", "1#2#3")
		).toDF("NAME", "PROD", "DATE", "RESULT")

		require(sd != null)
		require(testDF != null)
	}

	test("explode one column to multi column") {

		val plugin = ExplodePlugin("ExplodePlugin",
			PhMapArgs(Map(
				"splitColName" -> PhStringArgs("RESULT"),
				"delimiter" -> PhStringArgs("#")
			)),
			Seq()
		)

		val operator = AddColumnOperator(
			"AddColumnOperator",
			PhMapArgs(Map(
				"inDFName" -> PhStringArgs("inDFName"),
				"newColName" -> PhStringArgs("explode")
			)),
			Seq(plugin)
		)

		val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
		val df = result.toDFArgs.get

		df.show(false)
		assert(df.columns.contains("explode"))
		assert(df.count == testDF.count() * 3)
	}
}
