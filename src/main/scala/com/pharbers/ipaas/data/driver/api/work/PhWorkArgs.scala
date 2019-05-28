package com.pharbers.ipaas.data.driver.api.work

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 15:52
  */
sealed trait PhWorkArgs[+A] extends Product with Serializable {
    val args: A

    def get: A = args

    def isEmpty: Boolean = false

    final def isDefined: Boolean = !isEmpty

    @inline final def getOrElse[B >: A](default: => B): B = if (isEmpty) default else this.get
}

final case class PhBooleanArgs(args: Boolean) extends PhWorkArgs[Boolean]

final case class PhStringArgs(args: String) extends PhWorkArgs[String]

final case class PhListArgs[A <: PhWorkArgs[_]](args: List[A]) extends PhWorkArgs[List[A]]

final case class PhMapArgs[A <: PhWorkArgs[_]](args: Map[String, A]) extends PhWorkArgs[Map[String, A]] {
    def getAs[B <: PhWorkArgs[_]](key: String): B = args(key).asInstanceOf[B]
}

final case class PhRDDArgs[A](args: RDD[A]) extends PhWorkArgs[RDD[A]]

final case class PhDFArgs(args: DataFrame) extends PhWorkArgs[DataFrame]

final case class PhFuncArgs(args: PhWorkArgs[_] => PhWorkArgs[_]) extends PhWorkArgs[PhWorkArgs[_] => PhWorkArgs[_]]

case object PhNoneArgs extends PhWorkArgs[Unit] {
    override val args: Unit = Unit

    override def isEmpty = true
}