package com.pharbers.ipaas.data.driver.api.work

import com.pharbers.data.util.spark.sparkDriver
import com.pharbers.ipaas.data.driver.funcs.{PhOperatorArgs, PhPluginArgs}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame}
import org.bson.types.ObjectId

/**
  * @description:
  * @author: clock
  * @date: 2019-05-29 14:36
  */
object test extends App {
    sparkDriver.sc.addJar("target/ipaas-data-driver-0.1.jar")

    import sparkDriver.ss.implicits._
    import org.apache.spark.sql.functions._

    val df: DataFrame = List(
        ("name1", "prod1", "201801", 1),
        ("name2", "prod1", "201801", 2),
        ("name3", "prod2", "201801", 3),
        ("name4", "prod2", "201801", 4)
    ).toDF("NAME", "PROD", "DATE", "VALUE")

    case class plugin1() extends PhPluginArgs[Column] {
        val args: Column = lit("abcd")
    }

    case class plugin2() extends PhPluginArgs[Column] {
        val args: Column = col("VALUE").cast(DoubleType)
    }

    case class plugin3() extends PhPluginArgs[Column] {
        val args: Column = when($"DATE" === "201801", lit("去年"))
                .when($"DATE" === "201901", lit("今年"))
    }

    case class plugin4() extends PhPluginArgs[Column] {
        val generateIdUdf: UserDefinedFunction = udf { () => ObjectId.get().toString }
        val args: Column = generateIdUdf()
    }

    case class oper1(name: String, plugin: PhPluginArgs[Column]) extends PhOperatorArgs[DataFrame] {
        def perform(pr: DataFrame): DataFrame = {
            pr.withColumn(name, plugin.get)
        }

        override val args: DataFrame = df
    }

    case class action1() {
        val operatorLst: List[PhOperatorArgs[DataFrame]] = oper1("a", plugin1()) ::
                oper1("b", plugin2()) ::
                oper1("c", plugin3()) ::
                oper1("d", plugin4()) ::
                Nil

        def exec(): DataFrame = {
            operatorLst.foldLeft(df)((l, r) => r.perform(l))
        }

//        @macroUnfold
//        override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//            PhListArgs(operatorLst.map { x =>
//                x.name -> x.perform(PhNoneArgs).asInstanceOf[PhDFArgs]
//            })
//        }
    }

    val result = action1().exec().show(false)

//    case class action2() extends PhActionTrait {
//        val operatorLst: List[PhOperatorTrait] = operresult1 :: operresult2 :: Nil
//        override val name: String = "action1"
//        override val defaultArgs: PhWorkArgs[_] = PhNoneArgs
//
//        override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//            PhListArgs(operatorLst.map { x =>
//                x.name -> x.perform(PhNoneArgs).asInstanceOf[PhDFArgs]
//            })
//        }
//    }
//
//    val result = action1().perform(PhNoneArgs).asInstanceOf[PhListArgs[(String, PhDFArgs)]]
//    result.get.toMap[String, PhDFArgs].foreach(x => x._2.get.show(false))


//    val action2 = ???
//
//    val job1 = new PhJobTrait{
//        override val actionLst = action1 :: action2 :: Nil
//        val name: String = "job1"
//        val defaultArgs: PhWorkArgs[_] = PhStringArgs("job1defaultArgs")
//
//        def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
//            ???
//        }
//
//    }

//    job1.perform(PhNoneArgs)
}
