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

import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.plugins.ExprPlugin
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestAddDiffColsOperator extends FunSuite with BeforeAndAfterAll {
    implicit var sd: PhSparkDriver = _
    var testDF1: DataFrame = _
    var testDF2: DataFrame = _

    override def beforeAll(): Unit = {
        sd = PhSparkDriver("test-driver")
        val tmp = sd.ss.implicits
        import tmp._

        testDF1 = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod1", "201801", 2),
            ("name3", "prod2", "201801", 3),
            ("name4", "prod2", "201801", 4)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        testDF2 = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod1", "201801", 2),
            ("name3", "prod2", "201801", 3),
            ("name4", "prod2", "201801", 4)
        ).toDF("NAME2", "PROD2", "DATE2", "VALUE2")

        require(sd != null)
        require(testDF1 != null)
        require(testDF2 != null)
    }

    test("add diff cols") {
        val operator = AddDiffColsOperator(
            "AddDiffColsOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDF1"),
                "moreColDFName" -> PhStringArgs("inDF2")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map("inDF1" -> PhDFArgs(testDF1), "inDF2" -> PhDFArgs(testDF2))))
        result.get.show(false)
        assert(result.get.columns.length == 8)
    }
}
