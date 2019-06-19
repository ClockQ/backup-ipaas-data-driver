package com.pharbers.ipaas.data.driver.run

import com.pharbers.ipaas.data.driver.api.factory._
import com.pharbers.ipaas.data.driver.api.work._
import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.readParquet
import env.configObj
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
class TestNhwaMax2 extends FunSuite {
	test("test nhwa max") {
		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

		val jobs = configObj.readJobConfig("pharbers_config/max.yaml")
		val phJobs = jobs.map(x => getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
		val result = phJobs.head.perform(PhMapArgs(Map.empty))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
		val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///test/qi/qi/new_max_true")

		println(maxDF.count())
		println(maxTrueDF.count())

		println(maxDF.agg(sum("f_units")).first.get(0))
		println(maxDF.agg(sum("f_sales")).first.get(0))

		println(maxTrueDF.agg(sum("f_units")).first.get(0))
		println(maxTrueDF.agg(sum("f_sales")).first.get(0))
	}

	test("test pfizer CNS_R max") {
		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

		val jobs = configObj.readJobConfig("pharbers_config/pfizerCNS_RMax.yaml")
		val phJobs = jobs.map(x => getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
		val result = phJobs.head.perform(PhMapArgs(Map.empty))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
		val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/5c20fe92-fe5f-4ae5-802a-225791a135a2")

		println(maxDF.count())
		println(maxTrueDF.count())

		println(maxDF.agg(sum("f_units")).first.get(0))
		println(maxDF.agg(sum("f_sales")).first.get(0))

		println(maxTrueDF.agg(sum("f_units")).first.get(0))
		println(maxTrueDF.agg(sum("f_sales")).first.get(0))
	}

	test("test pfizer DVP max") {
		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

		val jobs = configObj.readJobConfig("pharbers_config/pfizerDVPMax.yaml")
		val phJobs = jobs.map(x => getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
		val result = phJobs.head.perform(PhMapArgs(Map.empty))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
//		val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/5c20fe92-fe5f-4ae5-802a-225791a135a2")

		println(maxDF.count())
//		println(maxTrueDF.count())

		println(maxDF.agg(sum("f_units")).first.get(0))
		println(maxDF.agg(sum("f_sales")).first.get(0))
		maxDF.show(false)
//		println(maxTrueDF.agg(sum("f_units")).first.get(0))
//		println(maxTrueDF.agg(sum("f_sales")).first.get(0))
	}

	test("test pfizer INF max") {
		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

		val jobs = Config.readJobConfig("pharbers_config/INF_Max.yaml")
		val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
		val result = phJobs.head.perform(PhMapArgs(Map.empty))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
		val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/e7a80161-8b69-4d14-82d5-eb5a988f00e8")

//		println(maxDF.count())
//		println(maxTrueDF.count())
//
//		println(maxDF.agg(sum("f_units")).first.get(0))
//		println(maxDF.agg(sum("f_sales")).first.get(0))
//		maxDF.show(false)
//		println(maxTrueDF.agg(sum("f_units")).first.get(0))
//		println(maxTrueDF.agg(sum("f_sales")).first.get(0))
//		println("config医院数量： " + maxDF.select("HOSPITAL_ID").distinct().count().toString)
//		println("config产品数量： " + maxDF.select("PRODUCT_ID").distinct().count().toString)
//
//		println("对照医院数量： " + maxTrueDF.select("Panel_ID").distinct().count().toString)
//		println("对照产品数量： " + maxTrueDF.select("Product").distinct().count().toString)
//
//		val prodErd = {
//			sd.setUtil(readParquet())
//				.readParquet("hdfs:///repository/prod_etc_dis_max/5ca069e2eeefcc012918ec73").filter(col("MARKET") === "INF")
//				.withColumn("min2", trim(regexp_replace(concat(col("ETC_PRODUCT_NAME"), col("ETC_DOSAGE_NAME"), col("ETC_PACKAGE_DES"), col("ETC_PACKAGE_NUMBER"), col("ETC_CORP_NAME")), " ", "")))
//		}
//		maxDF.select("PRODUCT_ID").distinct()
//			.join(prodErd, col("PRODUCT_ID") === col("ETC_PRODUCT_ID"))
//			.join(maxTrueDF.select("Product").distinct(), col("min2") === col("Product"), "left")
//			.filter(col("Product").isNull)
//			.show(40, false)

		val hospErd = {
			sd.setUtil(readParquet())
				.readParquet("hdfs:///repository/hosp_dis_max")
				//                    .readParquet("hdfs:///repository/hosp")
				.filter("PHA_IS_REPEAT == 0")
				.dropDuplicates(List("PHA_HOSP_ID"))
		}
		val a = maxDF.selectExpr("HOSPITAL_ID as hosp").join(hospErd, col("hosp") === col("HOSPITAL_ID"))
			.select("PHA_HOSP_ID")
			.join(maxTrueDF, col("PHA_HOSP_ID") === col("Panel_ID"))
			//                .filter(col("PHA_HOSP_ID").isNull)
			.drop("PHA_HOSP_ID")
			.distinct()
		println(a.agg(sum("f_units")).first.get(0))
		println(a.agg(sum("f_sales")).first.get(0))
	}

	test("test pfizer AI_D max") {
		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

		val jobs = Config.readJobConfig("pharbers_config/AI_D_Max.yaml")
		val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
		val result = phJobs.head.perform(PhMapArgs(Map.empty))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
		val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/34345082-4d8e-4625-82c8-95d603c75264")

		println(maxDF.count())
		println(maxTrueDF.count())

		println(maxDF.agg(sum("f_units")).first.get(0))
		println(maxDF.agg(sum("f_sales")).first.get(0))
		maxDF.show(false)
		println(maxTrueDF.agg(sum("f_units")).first.get(0))
		println(maxTrueDF.agg(sum("f_sales")).first.get(0))
	}

	test("test pfizer CNS_R max cleaned") {
		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

		val jobs = Config.readJobConfig("pharbers_config/CNS_R_Max.yaml")
		val phJobs = jobs.map(x => PhFactory.getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
		val result = phJobs.head.perform(PhMapArgs(Map.empty))

		val maxDF = result.toMapArgs[PhDFArgs].get("maxResultDF").get
		val maxTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Max/1dc050f8-2c49-4da9-ae1c-f8ff4ef2dec4")

		println(maxDF.count())
		println(maxTrueDF.count())

		println(maxDF.agg(sum("f_units")).first.get(0))
		println(maxDF.agg(sum("f_sales")).first.get(0))
		maxDF.show(false)
		println(maxTrueDF.agg(sum("f_units")).first.get(0))
		println(maxTrueDF.agg(sum("f_sales")).first.get(0))
	}

	test("test nhwa clean") {
		implicit val sd: PhSparkDriver = PhSparkDriver("testSparkDriver")

		val jobs = configObj.readJobConfig("pharbers_config/clean.yaml")
		val phJobs = jobs.map(x => getMethodMirror(x.getFactory)(x).asInstanceOf[PhJobFactory].inst())
		val result = phJobs.head.perform(PhMapArgs(Map.empty))

		val cleanDF = result.toMapArgs[PhDFArgs].get("clean").get
		val cleanTrueDF = sd.setUtil(readParquet()).readParquet("hdfs:///workData/Clean/20bfd585-c889-4385-97ec-a8d4c77d71cc")

		cleanDF.show(false)
		cleanTrueDF.show(false)

		println(cleanDF.count())
		println(cleanTrueDF.count())

		println(cleanDF.agg(sum("UNITS")).first.get(0))
		println(cleanDF.agg(sum("SALES")).first.get(0))

		println(cleanTrueDF.agg(sum("UNITS")).first.get(0))
		println(cleanTrueDF.agg(sum("SALES")).first.get(0))
	}
}
