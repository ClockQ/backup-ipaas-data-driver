package com.pharbers.ipaas.data.driver.operators

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.api.work.{PhMapArgs, PhOperatorTrait, PhSparkDriverArgs, PhStringArgs}

class TestReadParquetOperator extends FunSuite with BeforeAndAfterAll {
    implicit var sd: PhSparkDriver = _

    val parquetPath: String = "hdfs:///repository/hosp_dis_max"

    override def beforeAll(): Unit = {
        sd = PhSparkDriver("test-driver")
        require(sd != null)
    }

    test("read parquet") {
        val operator = ReadParquetOperator(
            "ReadParquetOperator",
            PhMapArgs(Map(
                "path" -> PhStringArgs(parquetPath)
            )),
            Seq.empty
        )

        val result = operator.perform(PhMapArgs(Map("sparkDriver" -> PhSparkDriverArgs(sd))))
        assert(0 != result.get.count())
    }
}
