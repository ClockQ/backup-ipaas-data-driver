package com.pharbers.ipaas.data.driver.api.work

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

<<<<<<< Updated upstream
/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 15:52
  */
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
=======
trait PhWorkArgs extends java.io.Serializable {
    type t
    def get : t
    def getBy[T <: PhWorkArgs]: T#t = this.asInstanceOf[T].get
    def getAs[T <: PhWorkArgs](index: String): T#t =
        this.asInstanceOf[MapArgs].get(index).asInstanceOf[T].get
}
//
//case class RDDArgs[T](rdd: RDD[T]) extends PhWorkArgs {
//    type t = RDD[T]
//    override def get: RDD[T] = rdd
//}
//
case class DFArgs(df: DataFrame) extends PhWorkArgs {
    type t = DataFrame
    override def get: DataFrame = df
}

case class StringArgs(str: String) extends PhWorkArgs {
    type t = String
    override def get: String = str
}
//
case class ListArgs(lst: List[PhWorkArgs]) extends PhWorkArgs {
    type t = List[PhWorkArgs]
    override def get: List[PhWorkArgs] = lst
}
//
case class MapArgs(map: Map[String, PhWorkArgs]) extends PhWorkArgs{
    type t = Map[String, PhWorkArgs]
    override def get: Map[String, PhWorkArgs] = map
}
//
//case class BooleanArgs(b: Boolean) extends PhWorkArgs {
//    type t = Boolean
//    override def get: Boolean = b
//}
//
//case class NoneArgFuncArgs[R](func: Unit => R) extends PhWorkArgs {
//    type t = Unit => R
//    override def get: Unit => R = func
//}
//
//case class SingleArgFuncArgs[T, R](func : T => R) extends PhWorkArgs {
//    type t = T => R
//    override def get: T => R = func
//}
//
//case class BinaryArgsFuncArgs[T1, T2, R](func: (T1, T2) => R) extends PhWorkArgs {
//    type t = (T1, T2) => R
//    override def get: (T1, T2) => R = func
//}
//
//object NULLArgs extends PhWorkArgs {
//    type t = Unit
//    override def get: Unit = Unit
//}
>>>>>>> Stashed changes
