package com.pharbers.ipaas.data.driver.funcs

import org.bson.types.ObjectId
import org.apache.spark.sql.functions.udf
import com.pharbers.ipaas.data.driver.api.work._
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 17:21
  */
case class generateObjectId[A <: PhWorkArgs[_]](override val name: String = "generateObjectId",
                                         override val defaultArgs: PhWorkArgs[_] = PhNoneArgs) extends PhOperatorTrait {

    val generateIdUdf: UserDefinedFunction = udf { () => ObjectId.get().toString }

    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
         PhColArgs(generateIdUdf())
    }
}
