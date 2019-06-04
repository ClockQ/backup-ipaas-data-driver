package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhNoneArgs, PhPluginTrait, PhWorkArgs}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.bson.types.ObjectId

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:21
  */
case class generateObjectId(override val name: String = "generateObjectId",
                            override val defaultArgs: PhWorkArgs[_] = PhNoneArgs) extends PhPluginTrait {

    val generateIdUdf: UserDefinedFunction = udf { () => ObjectId.get().toString }

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        PhColArgs(generateIdUdf())
    }
}
