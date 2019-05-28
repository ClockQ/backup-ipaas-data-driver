package com.pharbers.ipaas.data.driver.operators

import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

/**
  * @description:
  * @author: clock
  * @date: 2019-05-23 19:01
  */
case class CalcYTD() {
    def exec(df: DataFrame)(implicit sd: phSparkDriver): DataFrame = {
        import sd.ss.implicits._
        import org.apache.spark.sql.functions._

//        val schema = ArrayType(
//            StructType(
//                List(
//                    StructField("DATE", StringType, true),
//                    StructField("VALUE", DoubleType, true)
//                )
//            )
//        )
//
//
//        val ytd = udf((drLst: Seq[Row]) => {
//            drLst
//                    .map {
//                        case Row(x: String, y: Double) => (x, y)
//                        case Row(x: String, y: Int) => (x, y.toDouble)
//                        case Row(x: String, y: Long) => (x, y.toDouble)
//                    }
//                    .groupBy(_._1.take(4))
//                    .values
//                    .flatMap { lt =>
//                        var tmp = 0.0
//                        lt.sortBy(_._1).map { x =>
//                            tmp += x._2
//                            (x._1, tmp)
//                        }
//                    }
//        }, schema)
//
//        df.withColumn("YTD", df("sales"))
//                .groupBy("HOSPITAL_ID")
//                .agg(ytd(collect_list(struct("DATE", "YTD"))) as "tmp")
//                .withColumn("dr", explode($"tmp"))
//                .select("HOSPITAL_ID", "dr.DATE", "dr.VALUE")


        val ytd = udf((dvLst: Seq[Row]) => {
            dvLst.map{ dv =>
                val date = dv.getAs[String](0)
                val value = dv.getAs[Int](1)
                (date, value, dv.getAs[String](2))
            }
//                    .map {
//                        case Row(x: String, y: Double) => (x, y)
//                        case Row(x: String, y: Int) => (x, y.toDouble)
//                        case Row(x: String, y: Long) => (x, y.toDouble)
//                    }
//                    .groupBy(_._1.take(4))
//                    .values
//                    .flatMap { lt =>
//                        var tmp = 0.0
//                        lt.sortBy(_._1).map { x =>
//                            tmp += x._2
//                            (x._1, tmp)
//                        }
//                    }
        })

        df
                .withColumnRenamed("DATE", "DATE")
                .withColumnRenamed("sales", "VALUE")
                .groupBy("HOSPITAL_ID")
                .agg(ytd(collect_list(struct("DATE", "VALUE", "_id"))) as "DV")
                .withColumn("DV", explode($"DV"))
                .select("HOSPITAL_ID", "DV.*")
//                .agg(expr(""))
    }

}
