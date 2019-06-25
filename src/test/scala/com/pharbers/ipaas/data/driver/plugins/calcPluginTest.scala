package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class calcPluginTest extends FunSuite {
	implicit val sparkDriver: PhSparkDriver = PhSparkDriver("testSparkDriver")
    import sparkDriver.ss.implicits._
    val partitionColumnNames = List("PROD")
    val dateColName = "DATE"
    val valueColumnName = "VALUE"
    val outputColumnName = "RESULT"

    test("year growth by window"){

        val df: DataFrame = List(
            ("name1", "prod1", "201701", 1),
            ("name2", "prod2", "201701", 2),
            ("name3", "prod1", "201801", 2),
            ("name4", "prod2", "201801", 3)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201701", 1, 0.0),
            ("name2", "prod2", "201701", 2, 0.0),
            ("name3", "prod1", "201801", 2, 1.0),
            ("name4", "prod2", "201801", 3, 0.5)
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

        val yearGrowthPlugin = CalcYearGrowth().perform(PhMapArgs(Map(
			"dateColName" -> PhStringArgs(dateColName),
			"valueColumnName" -> PhStringArgs(valueColumnName),
			"partitionColumnNames" -> PhListArgs(partitionColumnNames.map(x => PhStringArgs(x)))
		))).asInstanceOf[PhColArgs].get



        val result = df.withColumn(outputColumnName, yearGrowthPlugin)
        result.show()
        assert(result.columns.contains(outputColumnName))
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }

    test("ring growth"){

        val df: DataFrame = List(
            ("name1", "prod1", "201801", 1),
            ("name2", "prod2", "201801", 2),
            ("name3", "prod1", "201802", 2),
            ("name4", "prod2", "201802", 3)
        ).toDF("NAME", "PROD", "DATE", "VALUE")

        val checkDf: DataFrame = List(
            ("name1", "prod1", "201801", 1, 0.0),
            ("name2", "prod2", "201801", 2, 0.0),
            ("name3", "prod1", "201802", 2, 1.0),
            ("name4", "prod2", "201802", 3, 0.5)
        ).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

        df.withColumn("test", when(expr("VALUE > '1'"), 0)).show()
        val growthPlugin = CalcRingGrowth().perform(PhMapArgs(Map(
			"dateColName" -> PhStringArgs(dateColName),
			"valueColumnName" -> PhStringArgs(valueColumnName),
			"partitionColumnNames" -> PhListArgs(partitionColumnNames.map(x => PhStringArgs(x)))
		))).asInstanceOf[PhColArgs].get

        val result = df.withColumn(outputColumnName, growthPlugin)
        result.show()
        assert(result.columns.contains(outputColumnName))
        assert(result.join(checkDf, col("CHECK_NAME") === col("NAME")).filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
    }

	test("addByWhen plugin"){
		val df: DataFrame = List(
			("name1", "prod1", "201801", 1),
			("name2", "prod2", "201801", 2),
			("name3", "prod1", "201802", 2),
			("name4", "prod2", "201802", 3)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201801", 1, 1),
			("name2", "prod2", "201801", 2, 2),
			("name3", "prod1", "201802", 2, 0),
			("name4", "prod2", "201802", 3, 0)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")


		val condition = "DATE == 201801"
		val trueValue = lit(col("VALUE"))
		val otherValue = lit(0)
		val growthPlugin = AddByWhen().perform(PhMapArgs(Map(
			"condition" -> PhStringArgs(condition),
			"trueValue" -> PhColArgs(trueValue),
			"otherValue" -> PhColArgs(otherValue)
		))).asInstanceOf[PhColArgs].get

		val result = df.withColumn(outputColumnName, growthPlugin)
		result.show()
		assert(result.columns.contains(outputColumnName))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME") && col("CHECK_PROD") === col("PROD"))
			.filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}

	test("EI plugin"){
		val df: DataFrame = List(
			("name1", "prod1", "201701", 0.25),
			("name2", "prod2", "201702", 0.5),
			("name3", "prod1", "201801", 1.0),
			("name4", "prod2", "201802", 1.0)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201701", 0.25, 0),
			("name2", "prod2", "201702", 0.5, 0),
			("name3", "prod1", "201801", 1.0, 4),
			("name4", "prod2", "201802", 1.0, 2)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")


		val growthPlugin = CalcEI().perform(PhMapArgs(Map(
			"valueColumnName" -> PhStringArgs(valueColumnName),
			"dateColName" -> PhStringArgs(dateColName),
			"partitionColumnNames" -> PhListArgs(partitionColumnNames.map(x => PhStringArgs(x)))
		))).asInstanceOf[PhColArgs].get

		val result = df.withColumn(outputColumnName, growthPlugin)
		result.show()
		assert(result.columns.contains(outputColumnName))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME") && col("CHECK_PROD") === col("PROD"))
			.filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}

	test("MAT plugin"){
		val df: DataFrame = List(
			("name1", "prod1", "201701", 1),
			("name2", "prod1", "201702", 1),
			("name3", "prod1", "201801", 1),
			("name4", "prod1", "201802", 1)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201701", 1, 1),
			("name2", "prod1", "201702", 1, 2),
			("name3", "prod1", "201801", 1, 2),
			("name4", "prod1", "201802", 1, 2)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")


		val growthPlugin = CalcMat().perform(PhMapArgs(Map(
			"valueColumnName" -> PhStringArgs(valueColumnName),
			"dateColName" -> PhStringArgs(dateColName),
			"partitionColumnNames" -> PhListArgs(partitionColumnNames.map(x => PhStringArgs(x)))
		))).asInstanceOf[PhColArgs].get

		val result = df.withColumn(outputColumnName, growthPlugin)
		result.show()
		assert(result.columns.contains(outputColumnName))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME") && col("CHECK_PROD") === col("PROD"))
			.filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}

	test("CalcRankByWindow plugin"){
		val df: DataFrame = List(
			("name1", "prod1", "201701", 1),
			("name2", "prod2", "201701", 2),
			("name3", "prod1", "201801", 2),
			("name4", "prod2", "201801", 1)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201701", 1, 2),
			("name2", "prod2", "201701", 2, 1),
			("name3", "prod1", "201801", 2, 1),
			("name4", "prod2", "201801", 1, 2)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

		val rankPartitionColumnNames = List("DATE")
		val growthPlugin = CalcRankByWindow().perform(PhMapArgs(Map(
			"dateColName" -> PhStringArgs(dateColName),
			"valueColumnName" -> PhStringArgs(valueColumnName),
			"partitionColumnNames" -> PhListArgs(rankPartitionColumnNames.map(x => PhStringArgs(x)))
		))).asInstanceOf[PhColArgs].get

		val result = df.withColumn(outputColumnName, growthPlugin)
		result.show()
		assert(result.columns.contains(outputColumnName))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME") && col("CHECK_PROD") === col("PROD"))
			.filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}

	test("CalcShare plugin"){
		val df: DataFrame = List(
			("name1", "prod1", "201701", 5),
			("name2", "prod2", "201701", 5),
			("name3", "prod1", "201801", 3),
			("name4", "prod2", "201801", 7)
		).toDF("NAME", "PROD", "DATE", "VALUE")

		val checkDf: DataFrame = List(
			("name1", "prod1", "201701", 5, 0.5),
			("name2", "prod2", "201701", 5, 0.5),
			("name3", "prod1", "201801", 3, 0.3),
			("name4", "prod2", "201801", 7, 0.7)
		).toDF("CHECK_NAME", "CHECK_PROD", "CHECK_DATE", "CHECK_VALUE", "CHECK_RESULT")

		val rankPartitionColumnNames = List("DATE")
		val growthPlugin = CalcShare().perform(PhMapArgs(Map(
			"valueColumnName" -> PhStringArgs(valueColumnName),
			"partitionColumnNames" -> PhListArgs(rankPartitionColumnNames.map(x => PhStringArgs(x)))
		))).asInstanceOf[PhColArgs].get

		val result = df.withColumn(outputColumnName, growthPlugin)
		result.show()
		assert(result.columns.contains(outputColumnName))
		assert(result.join(checkDf, col("CHECK_NAME") === col("NAME") && col("CHECK_PROD") === col("PROD"))
			.filter(col("RESULT") =!= col("CHECK_RESULT")).count() == 0)
	}
}
