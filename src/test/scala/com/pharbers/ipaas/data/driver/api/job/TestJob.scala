package com.pharbers.ipaas.data.driver.api.job

import org.apache.spark.sql.{Column, DataFrame}
import com.pharbers.ipaas.data.driver.api.work._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

class TestJob extends FunSuite with BeforeAndAfterAll {
    implicit var sd: PhSparkDriver = _

    var testDF: DataFrame = _

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

        require(sd != null)
        require(testDF != null)
    }

    override def afterAll(): Unit = {
        sd.stopSpark()
    }

    case class lit(name: String, defaultArgs: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait2[Any]])
            extends PhPluginTrait2[Column] {

        import org.apache.spark.sql.{functions => sf}

        private val value: String = defaultArgs.getAs[PhStringArgs]("value").get.get

        def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(sf.lit(value))
    }

    case class cast(name: String = "cast", defaultArgs: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait2[Any]] = Nil)
            extends PhPluginTrait2[Column] {

        import org.apache.spark.sql.{functions => sf}

        private val col: String = defaultArgs.getAs[PhStringArgs]("col").get.get
        private val value: String = defaultArgs.getAs[PhStringArgs]("value").get.get

        def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(sf.col(col).cast(value))
    }

    case class generateIdUdf(name: String = "udf", defaultArgs: PhMapArgs[PhWorkArgs[Any]], subPluginLst: Seq[PhPluginTrait2[Any]] = Nil)
            extends PhPluginTrait2[Column] {

        import org.apache.spark.sql.expressions.UserDefinedFunction
        import org.apache.spark.sql.{functions => sf}
        import org.bson.types.ObjectId

        private val generateIdUdf: UserDefinedFunction = sf.udf { () => ObjectId.get().toString }

        def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[Column] = PhColArgs(generateIdUdf())
    }

    case class withColumn(name: String, defaultArgs: PhMapArgs[PhWorkArgs[Any]], pluginLst: Seq[PhPluginTrait2[Column]])
            extends PhOperatorTrait2[DataFrame] {
        val inDFName: String = defaultArgs.getAs[PhStringArgs]("inDFName").get.get
        val newColName: String = defaultArgs.getAs[PhStringArgs]("newColName").get.get

        def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[DataFrame] = {
            val inDF = pr.getAs[PhDFArgs](inDFName).get.get
            val plugin = pluginLst.head.perform(pr).get
            PhDFArgs(inDF.withColumn(newColName, plugin))
        }
    }


    test("PhBaseAction") {

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

        val result = action1.perform(PhMapArgs(Map("df" -> PhDFArgs(testDF))))

        println(result)
        result.toDFArgs.get.show(false)
    }

    test("PhBaseJob") {

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
        val result = job1.perform(PhMapArgs(Map("df" -> PhDFArgs(testDF))))

        println(result)
        result.toMapArgs.getAs[PhDFArgs]("testAction1").get.get.show(false)
        result.toMapArgs.getAs[PhDFArgs]("testAction2").get.get.show(false)
        assert(result.toMapArgs.get.size === 3)
    }
}
