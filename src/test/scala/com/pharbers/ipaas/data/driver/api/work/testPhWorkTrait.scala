package com.pharbers.ipaas.data.driver.api.work

import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.{Column, DataFrame, Row}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 15:06
  */
class testPhWorkTrait extends FunSuite with BeforeAndAfterAll {
    implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

    var testDF: DataFrame = _
    var testRDD: RDD[Row] = _

    override def beforeAll(): Unit = {
        import sd.ss.implicits._
        testDF = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod1", "201801", 2),
            ("name3", "prod2", "201801", 3),
            ("name4", "prod2", "201801", 4)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        testRDD = testDF.rdd

        require(sd != null)
        require(testDF != null)
        require(testRDD != null)
    }

    override def afterAll(): Unit = {
        sd.stopSpark()
    }

    test("PhWorkTrait-DataFrame") {

        case class lit(name: String, args: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait2[Any]])
                extends PhPluginTrait2[Column] {

            import org.apache.spark.sql.{functions => sf}

            private val value: String = args.getAs[PhStringArgs]("value").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(sf.lit(value))
        }

        case class cast(name: String = "cast", args: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait2[Any]] = Nil)
                extends PhPluginTrait2[Column] {

            import org.apache.spark.sql.{functions => sf}

            private val col: String = args.getAs[PhStringArgs]("col").get.get
            private val value: String = args.getAs[PhStringArgs]("value").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(sf.col(col).cast(value))
        }

        case class generateIdUdf(name: String = "udf", args: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait2[Any]] = Nil)
                extends PhPluginTrait2[Column] {

            import org.bson.types.ObjectId
            import org.apache.spark.sql.{functions => sf}
            import org.apache.spark.sql.expressions.UserDefinedFunction

            private val generateIdUdf: UserDefinedFunction = sf.udf { () => ObjectId.get().toString }

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(generateIdUdf())
        }

        case class withColumn(name: String, args: PhMapArgs[PhWorkArgs[Any]], pluginLst: Seq[PhPluginTrait2[Column]])
                extends PhOperatorTrait2[DataFrame] {
            val inDFName: String = args.getAs[PhStringArgs]("inDFName").get.get
            val newColName: String = args.getAs[PhStringArgs]("newColName").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
                val inDF = pr.getAs[PhDFArgs](inDFName).get.get
                val plugin = pluginLst.head.perform(pr).get
                PhDFArgs(inDF.withColumn(newColName, plugin))
            }
        }

        case class action(name: String = "testAction",
                          args: PhMapArgs[PhWorkArgs[Any]],
                          operatorLst: Seq[PhOperatorTrait2[Any]]) extends PhActionTrait2 {
            override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
                operatorLst
                        .foldLeft(pr)((l, r) => PhMapArgs(l.get + (r.name -> r.perform(l))))
                        .get(operatorLst.last.name)
            }
        }

        case class job(name: String = "testJob",
                       args: PhMapArgs[PhWorkArgs[Any]],
                       actionLst: List[PhActionTrait2]) extends PhJobTrait2 {
            override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
                var mapArgs = pr
                actionLst.foreach { action =>
                    mapArgs = PhMapArgs(mapArgs.get + (action.name -> action.perform(mapArgs)))
                }
                mapArgs
            }
        }

        val action1 = action("testAction1", PhMapArgs(), List(
            withColumn("withColumn1",
                PhMapArgs(Map("inDFName" -> PhStringArgs("df"), "newColName" -> PhStringArgs("a"))),
                Seq(lit("", PhMapArgs(Map("value" -> PhStringArgs("lit+a"))), Nil))
            ),
            withColumn("withColumn2",
                PhMapArgs(Map("inDFName" -> PhStringArgs("withColumn1"), "newColName" -> PhStringArgs("b"))),
                Seq(cast("", PhMapArgs(Map("col" -> PhStringArgs("VALUE"), "value" -> PhStringArgs("double"))), Nil))
            ),
            withColumn("withColumn3",
                PhMapArgs(Map("inDFName" -> PhStringArgs("withColumn2"), "newColName" -> PhStringArgs("c"))),
                Seq(generateIdUdf("", PhMapArgs(), Nil))
            )
        ))

        val action2 = action("testAction2", PhMapArgs(), List(
            withColumn("withColumn1",
                PhMapArgs(Map("inDFName" -> PhStringArgs("testAction1"), "newColName" -> PhStringArgs("aa"))),
                Seq(lit("", PhMapArgs(Map("value" -> PhStringArgs("lit+a"))), Nil))
            ),
            withColumn("withColumn2",
                PhMapArgs(Map("inDFName" -> PhStringArgs("withColumn1"), "newColName" -> PhStringArgs("bb"))),
                Seq(cast("", PhMapArgs(Map("col" -> PhStringArgs("VALUE"), "value" -> PhStringArgs("double"))), Nil))
            ),
            withColumn("withColumn3",
                PhMapArgs(Map("inDFName" -> PhStringArgs("withColumn2"), "newColName" -> PhStringArgs("cc"))),
                Seq(generateIdUdf("", PhMapArgs(), Nil))
            )
        ))


        val job1 = job("testJob", PhMapArgs(), List(action1, action2))
        val result = job1.perform(PhMapArgs(Map("df" -> PhDFArgs(testDF))))
        println(result)
        result.toMapArgs.getAs[PhDFArgs]("testAction1").get.get.show(false)
        result.toMapArgs.getAs[PhDFArgs]("testAction2").get.get.show(false)
    }

    test("PhWorkTrait-RDD") {

        case class addOne(name: String, args: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait2[Any]])
                extends PhPluginTrait2[Any => Any] {
            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any => Any] = PhFuncArgs( (x: Any) => x )
        }

        case class map(name: String, args: PhMapArgs[PhWorkArgs[Any]], pluginLst: Seq[PhPluginTrait2[Any => Any]])
                extends PhOperatorTrait2[RDD[_]] {
            val inRDDName: String = args.getAs[PhStringArgs]("inRDDName").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[RDD[_]] = {
                val inRDD = pr.getAs[PhRDDArgs[_]](inRDDName).get.get
                val plugin = pluginLst.head.perform(pr).get
                PhRDDArgs(inRDD.map(plugin))
            }
        }

        case class action(name: String = "testAction",
                          args: PhMapArgs[PhWorkArgs[Any]],
                          operatorLst: Seq[PhOperatorTrait2[Any]]) extends PhActionTrait2 {
            override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
                operatorLst.foldLeft(pr)((l, r) => PhMapArgs(l.get + (r.name -> r.perform(l))))
                        .get(operatorLst.last.name)
            }
        }

        case class job(name: String = "testJob",
                       args: PhMapArgs[PhWorkArgs[Any]],
                       actionLst: List[PhActionTrait2]) extends PhJobTrait2 {
            override def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any] = {
                var mapArgs = pr.toMapArgs[PhWorkArgs[_]]
                actionLst.foreach { action =>
                    mapArgs = PhMapArgs(mapArgs.get + (action.name -> action.perform(mapArgs)))
                }
                mapArgs
            }
        }

        val action1 = action("testAction1", PhMapArgs(), List(
            map("map1",
                PhMapArgs(Map("inRDDName" -> PhStringArgs("rdd"))),
                Seq(addOne("", PhMapArgs(), Nil))
            ),
            map("map2",
                PhMapArgs(Map("inRDDName" -> PhStringArgs("map1"), "newColName" -> PhStringArgs("b"))),
                Seq(addOne("", PhMapArgs(Map("col" -> PhStringArgs("VALUE"), "value" -> PhStringArgs("double"))), Nil))
            ),
            map("map3",
                PhMapArgs(Map("inRDDName" -> PhStringArgs("map2"), "newColName" -> PhStringArgs("c"))),
                Seq(addOne("", PhMapArgs(), Nil))
            )
        ))

        val action2 = action("testAction2", PhMapArgs(), List(
            map("map1",
                PhMapArgs(Map("inRDDName" -> PhStringArgs("testAction1"), "newColName" -> PhStringArgs("aa"))),
                Seq(addOne("", PhMapArgs(Map("value" -> PhStringArgs("lit+a"))), Nil))
            ),
            map("map2",
                PhMapArgs(Map("inRDDName" -> PhStringArgs("map1"), "newColName" -> PhStringArgs("bb"))),
                Seq(addOne("", PhMapArgs(Map("col" -> PhStringArgs("VALUE"), "value" -> PhStringArgs("double"))), Nil))
            ),
            map("map3",
                PhMapArgs(Map("inRDDName" -> PhStringArgs("map2"), "newColName" -> PhStringArgs("cc"))),
                Seq(addOne("", PhMapArgs(), Nil))
            )
        ))

        val job1 = job("testJob", PhMapArgs(), List(action1, action2))
        val result = job1.perform(PhMapArgs(Map("rdd" -> PhRDDArgs(testDF.rdd))))
        println(result)
    }
}
