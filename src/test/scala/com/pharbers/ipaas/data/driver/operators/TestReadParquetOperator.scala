package com.pharbers.ipaas.data.driver.operators

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait2, PhSparkDriverArgs, PhStringArgs}

class TestReadParquetOperator extends FunSuite with BeforeAndAfterAll {
    var operator: PhOperatorTrait2[_] = _

    val parquetPath: String = "hdfs:///repository/hosp_dis_max"

    override def beforeAll(): Unit = {
        operator = ReadParquetOperator(
            "ReadParquetOperator",
            PhMapArgs(Map(
                "path" -> PhStringArgs(parquetPath)
            )),
            Seq.empty
        )

        require(operator != null)
    }

    test("read parquet") {
        val result = operator.perform(PhMapArgs(Map("sparkDriver" -> PhSparkDriverArgs(PhSparkDriver("test")))))
        assert(0 != result.toDFArgs.get.count())
    }
}
