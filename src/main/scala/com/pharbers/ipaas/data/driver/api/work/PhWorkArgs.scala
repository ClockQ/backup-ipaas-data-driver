package com.pharbers.ipaas.data.driver.api.work

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

sealed trait PhWorkArgs[+A] extends Product with Serializable {
    val args: A

    def get: A

    def isEmpty: Boolean = false

    final def isDefined: Boolean = !isEmpty

    @inline final def getOrElse[B >: A](default: => B): B =
        if (isEmpty) default else this.get
}

final case class PhBooleanArgs(args: Boolean) extends PhWorkArgs[Boolean] {
    override def get: Boolean = args
}

final case class PhStringArgs(args: String) extends PhWorkArgs[String] {
    override def get: String = args
}

final case class PhListArgs[A](args: List[A]) extends PhWorkArgs[List[A]] {
    override def get: List[A] = args
}

final case class PhMapArgs[A](args: Map[String, A]) extends PhWorkArgs[Map[String, A]] {
    override def get: Map[String, A] = args
}

final case class PhRDDArgs[A](args: RDD[A]) extends PhWorkArgs[RDD[A]] {
    override def get: RDD[A] = args
}

final case class PhDFArgs(args: DataFrame) extends PhWorkArgs[DataFrame] {
    override def get: DataFrame = args
}

final case class PhFuncArgs(args: PhWorkArgs[_] => PhWorkArgs[_]) extends PhWorkArgs[PhWorkArgs[_] => PhWorkArgs[_]] {
    override def get: PhWorkArgs[_] => PhWorkArgs[_] = args
}

case object PhNoneArgs extends PhWorkArgs[Unit] {
    override val args: Unit = Unit

    override def isEmpty = true

    def get: Unit = Unit
}