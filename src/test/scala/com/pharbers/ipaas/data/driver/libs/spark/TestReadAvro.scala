package com.pharbers.ipaas.data.driver.libs.spark

import java.text.SimpleDateFormat
import java.util.Date

import com.pharbers.ipaas.data.driver.libs.spark.util.{readAvro, save2Avro}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TestReadAvro extends FunSuite {
    implicit val sd: PhSparkDriver = PhSparkDriver("test-driver")

    test("read and save avro test") {
        val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
        import sd.ss.implicits._

        val df1: DataFrame = List(
            ("name1", "prod1", "201701", 0.25),
            ("name2", "prod2", "201702", 0.5),
            ("name3", "prod1", "201801", 1.0),
            ("name4", "prod2", "201802", 1.0)
        ).toDF("NAME", "PROD", "DATE", "VALUE")
        val path = "hdfs:///test/avroTest/" + iString
        sd.setUtil(save2Avro()).save2Avro(df1, path)

        val df = sd.setUtil(readAvro()).readAvro(path)
        df.printSchema()
        df.show(false)
    }
}
