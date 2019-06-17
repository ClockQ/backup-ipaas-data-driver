package com.pharbers.ipaas.data.driver.plugins

import org.bson.types.ObjectId
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhPluginTrait2, PhWorkArgs}

/** 根据 MongoDB 的 oid 算法生成字段
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/17 18:50
  */
case class GenerateObjectIdPlugin(name: String,
                                  defaultArgs: PhMapArgs[PhWorkArgs[Any]],
                                  subPluginLst: Seq[PhPluginTrait2[Column]])
        extends PhPluginTrait2[Column] {

    val generateIdUdf: UserDefinedFunction = udf { () => ObjectId.get().toString }

    override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = {
        PhColArgs(generateIdUdf())
    }
}
