package com.pharbers.ipaas.data.driver.api.work

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}

trait PhWorkArgs[+A] extends Serializable {
    def get: A

    def isEmpty: Boolean = false

    final def isDefined: Boolean = !isEmpty
}

final case class PhBooleanArgs(args: Boolean = false) extends PhWorkArgs[Boolean] {
    def get: Boolean = args
}

final case class PhStringArgs(args: String = "") extends PhWorkArgs[String] {
    def get: String = args
}

final case class PhListArgs[+A: ClassTag](args: List[A] = Nil) extends PhWorkArgs[List[A]] {
    def get: List[A] = args
}

final case class PhMapArgs[+A: ClassTag](args: Map[String, A] = Map[String, Nothing]().empty) extends PhWorkArgs[Map[String, A]] {
    def get: Map[String, A] = args

    def getAs[B](key: String): Option[B] = args.get(key) match {
        case Some(one) => Some(one.asInstanceOf[B])
        case None => None
    }
}

final case class PhRDDArgs[A: ClassTag](args: RDD[A]) extends PhWorkArgs[RDD[A]] {
    def get: RDD[A] = args
}

final case class PhDFArgs(args: DataFrame) extends PhWorkArgs[DataFrame] {
    def get: DataFrame = args
}

final case class PhColArgs(args: Column) extends PhWorkArgs[Column] {
    def get: Column = args
}

final case class PhFuncArgs[A: ClassTag, B: ClassTag](args: A => B) extends PhWorkArgs[A => B] {
    def get: A => B = args
}

case object PhNoneArgs extends PhWorkArgs[Nothing] {
    def get: Nothing = throw new NoSuchElementException("PhNoneArgs.get")

    override def isEmpty = true
}

