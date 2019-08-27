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

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}

class TestDistinctOperator extends FunSuite with BeforeAndAfterAll {

    import env.sparkObj._
    import sparkDriver.ss.implicits._

    var testDF: DataFrame = _

    override def beforeAll(): Unit = {
        testDF = List(
            ("name1", "prod1", "201801", 1),
            ("name1", "prod1", "201801", 1),
            ("name2", "prod2", "201801", 2),
            ("name2", "prod2", "201801", 2)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        require(testDF != null)
    }

    test("distinct") {
        val operator = DistinctOperator(
            "DistinctOperator",
            PhMapArgs(Map(
                "inDFName" -> PhStringArgs("inDFName")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
        assert(result.get.count() == 2)
    }
}
