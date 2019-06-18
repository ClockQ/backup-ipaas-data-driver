//package com.pharbers.ipaas.data.driver.plugins
//
//import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhNoneArgs, PhPluginTrait, PhStringArgs, PhWorkArgs}
//import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.functions.udf
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.functions._
//
///** id to oid 插件
//  *
//  * @author clock
//  * @version 0.1
//  * @since 2019/6/17 18:50
//  */
//case class Id2Oid(override val name: String = "Id2Oid",
//                  override val defaultArgs: PhWorkArgs[_] = PhNoneArgs) extends PhPluginTrait {
//
//
//    val trimOIdUdf: UserDefinedFunction = udf(oidSchema)
//
//    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//        val argsMap = defaultArgs.asInstanceOf[PhMapArgs[_]]
//        val idColName = argsMap.getAs[PhStringArgs]("idColName").get.get
//        PhColArgs(trimOIdUdf(col(idColName)))
//    }
//}
//
///** oidSchema
//  *
//  * @author clock
//  * @version 0.1
//  * @since 2019/6/17 18:50
//  */
//private[plugins] case class oidSchema(oid: String) {
//    val oidSchema = StructType(StructField("oid", StringType, nullable = false) :: Nil)
//    new GenericRowWithSchema(Array(oid), oidSchema)
//}