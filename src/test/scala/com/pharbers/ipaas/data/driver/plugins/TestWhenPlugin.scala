///*
// * This file is part of com.pharbers.ipaas-data-driver.
// *
// * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
// */
//
//package com.pharbers.ipaas.data.driver.plugins
//
//import com.pharbers.ipaas.data.driver.api.work.{PhDFArgs, PhMapArgs, PhStringArgs}
//import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
//import com.pharbers.ipaas.data.driver.operators.AddColumnOperator
//import org.apache.spark.sql.DataFrame
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
//class TestWhenPlugin extends FunSuite with BeforeAndAfterAll {
//    implicit var sd: PhSparkDriver = _
//    var testDF: DataFrame = _
//
//    override def beforeAll(): Unit = {
//        sd = PhSparkDriver("test-driver")
//        val tmp = sd.ss.implicits
//        import tmp._
//
//        testDF = List(
//            ("name1", "prod1", "2018", "1"),
//            ("name2", "prod1", "2018", "2"),
//            ("name3", "prod2", "2018", "3"),
//            ("name4", "prod2", "2018", "4")
//        ).toDF("NAME", "PROD", "DATE", "VALUE")
//
//        require(sd != null)
//        require(testDF != null)
//    }
//
//    test("odd number add one, even number add two") {
//        val plugin = WhenPlugin(
//            "WhenPlugin",
//            PhMapArgs(Map(
//                "condition" -> PhStringArgs("VALUE % 2 == 1"),
//                "value" -> PhStringArgs("VALUE + 1")
//            )),
//            Seq(
//                WhenPlugin(
//                    "WhenPlugin",
//                    PhMapArgs(Map(
//                        "condition" -> PhStringArgs("VALUE % 2 == 0"),
//                        "value" -> PhStringArgs("VALUE + 2")
//                    )),
//                    Seq()
//                )
//            )
//        )
//
//        val operator = AddColumnOperator(
//            "AddColumnOperator",
//            PhMapArgs(Map(
//                "inDFName" -> PhStringArgs("inDFName"),
//                "newColName" -> PhStringArgs("add")
//            )),
//            Seq(plugin)
//        )
//        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
//        val df = result.toDFArgs.get
//        df.show(false)
//
//        assert(df.columns.contains("add"))
//        df.collect().foreach { x =>
//            val value = x.getString(3).toDouble
//            val add = x.getString(4).toDouble
//            if (value % 2 == 1)
//                assert(value + 1 == add)
//            else
//                assert(value + 2 == add)
//        }
//    }
//
//    test("odd number add one, even number is name") {
//        val plugin = WhenPlugin(
//            "WhenPlugin",
//            PhMapArgs(Map(
//                "condition" -> PhStringArgs("VALUE % 2 == 1"),
//                "value" -> PhStringArgs("VALUE + 1")
//            )),
//            Seq(
//                ExprPlugin(
//                    "ExprPlugin",
//                    PhMapArgs(Map(
//                        "exprString" -> PhStringArgs("NAME")
//                    )),
//                    Seq()
//                )
//            )
//        )
//
//        val operator = AddColumnOperator(
//            "AddColumnOperator",
//            PhMapArgs(Map(
//                "inDFName" -> PhStringArgs("inDFName"),
//                "newColName" -> PhStringArgs("add")
//            )),
//            Seq(plugin)
//        )
//        val result = operator.perform(PhMapArgs(Map("inDFName" -> PhDFArgs(testDF))))
//        val df = result.toDFArgs.get
//        df.show(false)
//
//        assert(df.columns.contains("add"))
//        df.collect().foreach { x =>
//            val value = x.getString(3).toDouble
//            val add = x.getString(4)
//            val name = x.getString(0)
//            if (value % 2 == 1)
//                assert(value + 1 == add.toDouble)
//            else
//                assert(add == name)
//        }
//    }
//}
