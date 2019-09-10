package com.pharbers.ipaas.data.driver.operators

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait, PhSparkDriverArgs, PhStringArgs}

class TestReadCsvOperator extends FunSuite with BeforeAndAfterAll {

    import env.sparkObj._
    import sparkDriver.ss.implicits._

    var operator: PhOperatorTrait[_] = _

    val csvPath: String = "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_CPA_20181126.csv"

    override def beforeAll(): Unit = {
        operator = ReadCsvOperator(
            "ReadCsvOperator",
            PhMapArgs(Map(
                "path" -> PhStringArgs(csvPath),
                "delimiter" -> PhStringArgs(",")
            )),
            Seq.empty
        )

        require(operator != null)
    }

    test("read csv") {
        val result = operator.perform(PhMapArgs(Map("sparkDriver" -> PhSparkDriverArgs(PhSparkDriver("test")))))
        assert(0 != result.toDFArgs.get.count())
    }
}
