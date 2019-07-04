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

package com.pharbers.ipaas.kafka.relay.operators

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhStringArgs}

class TestCreateSourceConnectOperator extends FunSuite with BeforeAndAfterAll {

    val local = "http://192.168.100.176:8083"

    test("create source connect") {
        val operator = CreateSourceConnectOperator(
            "CreateSourceConnectOperator",
            PhMapArgs(Map(
                "connectName" -> PhStringArgs("fs-source-connector1"),
                "topic" -> PhStringArgs("testTopic"),
                "listenFile" -> PhStringArgs("/data/test.log")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map()))
        result.get(local)
    }

    test("delete source connect") {
        val operator = DeleteConnectOperator(
            "DeleteConnectOperator",
            PhMapArgs(Map(
                "connectName" -> PhStringArgs("fs-source-connector1")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map()))
        result.get(local)
    }

    test("create sink connect") {
        val operator = CreateSinkConnectOperator(
            "CreateSinkConnectOperator",
            PhMapArgs(Map(
                "connectName" -> PhStringArgs("fs-sink-connector1"),
                "topic" -> PhStringArgs("testTopic"),
                "listenFile" -> PhStringArgs("/data/test-sink2.log")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map()))
        result.get(local)
    }

    test("delete sink connect") {
        val operator = DeleteConnectOperator(
            "DeleteConnectOperator",
            PhMapArgs(Map(
                "connectName" -> PhStringArgs("fs-sink-connector1")
            )),
            Seq()
        )
        val result = operator.perform(PhMapArgs(Map()))
        result.get(local)
    }
}
