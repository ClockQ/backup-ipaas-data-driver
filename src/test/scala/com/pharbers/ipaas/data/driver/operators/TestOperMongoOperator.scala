package com.pharbers.ipaas.data.driver.operators

import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait, PhSparkDriverArgs, PhStringArgs}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestOperMongoOperator extends FunSuite with BeforeAndAfterAll {

    import env.sparkObj._

    var operator: PhOperatorTrait[_] = _

    val mongodbHost: String = "192.168.100.176"
    val mongodbPort: String = "27017"
    val databaseName: String = "pharbers-max-repository"
    val collName: String = "chc"

    override def beforeAll(): Unit = {
        operator = ReadMongoOperator(
            "ReadMongoOperator",
            PhMapArgs(Map(
                "mongodbHost" -> PhStringArgs(mongodbHost),
                "mongodbPort" -> PhStringArgs(mongodbPort),
                "databaseName" -> PhStringArgs(databaseName),
                "collName" -> PhStringArgs(collName)
            )),
            Seq.empty
        )

        require(operator != null)
    }

    test("read mongo") {
        val result = operator.perform(PhMapArgs(Map()))
        assert(0 != result.toDFArgs.get.count())
    }
}
