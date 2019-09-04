package com.pharbers.ipaas.data.driver.libs.spark

import org.scalatest.FunSuite
import org.apache.spark.sql.SaveMode
import com.pharbers.ipaas.data.driver.libs.spark.util.{readEs, save2Es}

class TestSparkES extends FunSuite {
    implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")

    val esHost = "127.0.01"
    val esPort = "9200"
    val r_index = "r_index"
    val w_index = "w_index"
    val query = "?q=hostname:IronMan"

    test("读写 ES 的数据") {
        val result = sd.setUtil(readEs()).readEs(esHost, esPort, r_index, query)
        assert(result.count() != 0)

        println(result.count())
        result.show(false)

//        sd.setUtil(save2Es()).save2Es(result, esHost, esPort, w_index, SaveMode.Append)
    }
}
