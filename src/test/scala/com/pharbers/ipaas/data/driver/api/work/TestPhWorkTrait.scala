package com.pharbers.ipaas.data.driver.api.work

import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.{Column, DataFrame, Row}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.api.job.{PhBaseAction, PhBaseJob}
import com.pharbers.ipaas.data.driver.libs.log.{PhLogFormat, formatMsg}

class TestPhWorkTrait extends FunSuite with BeforeAndAfterAll {
    implicit var sd: PhSparkDriver = _

    var testDF: DataFrame = _
    var testRDD: RDD[Row] = _

    override def beforeAll(): Unit = {
        sd = PhSparkDriver("test-driver")
        val tmp = sd.ss.implicits
        import tmp._
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

        case class lit(name: String, defaultArgs: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait[Any]])
                extends PhPluginTrait[Column] {

            import org.apache.spark.sql.{functions => sf}

            private val value: String = defaultArgs.getAs[PhStringArgs]("value").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(sf.lit(value))
        }

        case class cast(name: String = "cast", defaultArgs: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait[Any]] = Nil)
                extends PhPluginTrait[Column] {

            import org.apache.spark.sql.{functions => sf}

            private val col: String = defaultArgs.getAs[PhStringArgs]("col").get.get
            private val value: String = defaultArgs.getAs[PhStringArgs]("value").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(sf.col(col).cast(value))
        }

        case class generateIdUdf(name: String = "udf", defaultArgs: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait[Any]] = Nil)
                extends PhPluginTrait[Column] {

            import org.bson.types.ObjectId
            import org.apache.spark.sql.{functions => sf}
            import org.apache.spark.sql.expressions.UserDefinedFunction

            private val generateIdUdf: UserDefinedFunction = sf.udf { () => ObjectId.get().toString }

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(generateIdUdf())
        }

        case class withColumn(name: String, defaultArgs: PhMapArgs[PhWorkArgs[Any]], pluginLst: Seq[PhPluginTrait[Column]])
                extends PhOperatorTrait[DataFrame] {
            val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
            val newColName: String = defaultArgs.getAs[PhStringArgs]("newColName").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
                val inDF = pr.getAs[PhDFArgs](inDFName).get.get
                val plugin = pluginLst.head.perform(pr).get
                PhDFArgs(inDF.withColumn(newColName, plugin))
            }
        }

        val action1 = PhBaseAction("testAction1", PhMapArgs(), List(
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

        val action2 = PhBaseAction("testAction2", PhMapArgs(), List(
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


        val job1 = PhBaseJob("testJob", PhMapArgs(), List(action1, action2))
        val result = job1.perform(PhMapArgs(Map(
            "df" -> PhDFArgs(testDF),
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID","test_jobId")).get()
        )))

        println(result)
        result.toMapArgs.getAs[PhDFArgs]("testAction1").get.get.show(false)
        result.toMapArgs.getAs[PhDFArgs]("testAction2").get.get.show(false)
        assert(result.toMapArgs.get.size == 5)
        assert(result.toMapArgs.getAs[PhDFArgs]("testAction1").get.get.columns.length == 7)
        assert(result.toMapArgs.getAs[PhDFArgs]("testAction2").get.get.columns.length == 10)
    }

    test("PhWorkTrait-RDD") {

        case class addOne(name: String, defaultArgs: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait[Any]])
                extends PhPluginTrait[Any => Any] {
            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Any => Any] = PhFuncArgs((x: Any) => x)
        }

        case class map(name: String, defaultArgs: PhMapArgs[PhWorkArgs[Any]], pluginLst: Seq[PhPluginTrait[Any => Any]])
                extends PhOperatorTrait[RDD[_]] {
            val inRDDName: String = defaultArgs.getAs[PhStringArgs]("inRDDName").get.get

            def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[RDD[_]] = {
                val inRDD = pr.getAs[PhRDDArgs[_]](inRDDName).get.get
                val plugin = pluginLst.head.perform(pr).get
                PhRDDArgs(inRDD.map(plugin))
            }
        }

        val action1 = PhBaseAction("testAction1", PhMapArgs(), List(
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

        val action2 = PhBaseAction("testAction2", PhMapArgs(), List(
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

        val job1 = PhBaseJob("testJob", PhMapArgs(), List(action1, action2))
        val result = job1.perform(PhMapArgs(Map(
            "rdd" -> PhRDDArgs(testDF.rdd),
            "sparkDriver" -> PhSparkDriverArgs(sd),
            "logFormat" -> PhLogFormat(formatMsg("test_user", "test_traceID", "test_jobId")).get()
        )))

        println(result)
        assert(result.toMapArgs.get.size == 5)
    }
}
