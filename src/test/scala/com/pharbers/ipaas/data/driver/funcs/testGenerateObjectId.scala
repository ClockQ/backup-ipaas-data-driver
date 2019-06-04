package com.pharbers.ipaas.data.driver.funcs

import env.sparkObj
import com.pharbers.ipaas.data.driver.api.work
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.operators.addColumn
import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 20:12
  */
object testGenerateObjectId extends App {
    sparkObj.sc.addJar("target/ipaas-data-driver-0.1.jar")
    import sparkObj.ss.implicits._

    val df: DataFrame = List(
        ("name1", "prod1", "201801", 1),
        ("name2", "prod1", "201801", 2),
        ("name3", "prod2", "201801", 3),
        ("name4", "prod2", "201801", 4)
    ).toDF("NAME", "PROD", "DATE", "VALUE")

//    val test = ??? //addColumn()
//    test.perform(PhMapArgs(Map(
//        "inDFName" -> PhStringArgs("inDF"),
//        "outDFName" -> PhStringArgs("outDF"),
//        "newColName" -> PhStringArgs("newCol"),
//        "funcName" -> PhStringArgs("func"),
//        "inDF" -> PhDFArgs(df),
//        "func" -> work.PhFuncArgs(generateObjectId().perform(_))
//    )))
}
