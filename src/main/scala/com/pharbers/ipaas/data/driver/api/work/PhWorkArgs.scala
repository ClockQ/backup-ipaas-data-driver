package com.pharbers.ipaas.data.driver.api.work

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}

/** iPaas Driver 的统一参数包装类，是所有包装类的父类，相当于Any
  *
  * @param args 实际包装的参数
  * @tparam A 包装的参数类型
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:37
  * @note
  */
trait PhWorkArgs[+A] extends Serializable {
    /** 获取实际包装的参数
      *
      * @return A 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 14:41
      */
    def get: A

    /** 判断包装的参数是否为空
      *
      * @return Boolean 未包装任何参数则返回true
      * @author clock
      * @version 0.1
      * @since 2019/6/11 14:42
      */
    def isEmpty: Boolean = false

    /** 判断包装的参数是否有效
      *
      * @return Boolean 包装任意参数则返回true
      * @author clock
      * @version 0.1
      * @since 2019/6/11 14:43
      */
    final def isDefined: Boolean = !isEmpty
}

/** iPaas Driver 的Boolean参数包装类
  *
  * @param args 实际包装的Boolean参数，默认值为false
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:44
  * @note
  */
final case class PhBooleanArgs(args: Boolean = false) extends PhWorkArgs[Boolean] {
    /** 获取实际包装的Boolean参数
      *
      * @return Boolean 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 14:45
      */
    def get: Boolean = args
}

/** iPaas Driver 的String参数包装类
  *
  * @param args 实际包装的String参数，默认值为“”
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:50
  * @note
  */
final case class PhStringArgs(args: String = "") extends PhWorkArgs[String] {
    /** 获取实际包装的String参数
      *
      * @return String 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 14:57
      */
    def get: String = args
}

/** iPaas Driver 的List参数包装类
  *
  * @param args 实际包装的List参数，默认值为Nil
  * @tparam A List中elem的类型参数
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:51
  * @note
  */
final case class PhListArgs[+A: ClassTag](args: List[A] = Nil) extends PhWorkArgs[List[A]] {
    /** 获取实际包装的List参数
      *
      * @tparam A List中elem的类型参数
      * @return List[A] 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 14:58
      */
    def get: List[A] = args
}

/** iPaas Driver 的Map参数包装类
  *
  * @param args 实际包装的Map参数，默认值为Map[String, Nothing]()
  * @tparam A Map中value的类型参数
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:53
  * @note
  */
final case class PhMapArgs[+A: ClassTag](args: Map[String, A] = Map[String, Nothing]().empty) extends PhWorkArgs[Map[String, A]] {
    /** 获取实际包装的Map参数
      *
      * @tparam A Map中value的类型参数
      * @return Map[String, A] 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 14:59
      */
    def get: Map[String, A] = args

    /** 获取Map中某个key的值
      *
      * @tparam B Map中value的类型参数
      * @return Option[B] 返回某个key的结果选项
      * @author clock
      * @version 0.1
      * @since 2019/6/11 15:00
      */
    def getAs[B](key: String): Option[B] = args.get(key) match {
        case Some(one) => Some(one.asInstanceOf[B])
        case None => None
    }
}

/** iPaas Driver 的RDD参数包装类
  *
  * @param args 实际包装的RDD参数，没有默认值
  * @tparam A RDD中elem的类型参数
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:54
  * @note
  */
final case class PhRDDArgs[A: ClassTag](args: RDD[A]) extends PhWorkArgs[RDD[A]] {
    /** 获取实际包装的RDD参数
      *
      * @tparam A RDD中elem的类型参数
      * @return RDD[A] 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 15:02
      */
    def get: RDD[A] = args
}

/** iPaas Driver 的DataFrame参数包装类
  *
  * @param args 实际包装的DataFrame参数，没有默认值
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:54
  * @note
  */
final case class PhDFArgs(args: DataFrame) extends PhWorkArgs[DataFrame] {

    /** 获取实际包装的DataFrame参数
      *
      * @return DataFrame 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 15:03
      */
    def get: DataFrame = args
}

/** iPaas Driver 的Column参数包装类
  *
  * @param args 实际包装的Column参数，没有默认值
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:55
  * @note
  */
final case class PhColArgs(args: Column) extends PhWorkArgs[Column] {
    /** 获取实际包装的Column参数
      *
      * @return Column 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 15:03
      */
    def get: Column = args
}

/** iPaas Driver 的Func参数包装类
  *
  * @param args 实际包装的Func参数，没有默认值
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:56
  * @note 表示一个 A => B 的通项公式，如果A为空，则表示为() => B 或 Unit => B
  */
final case class PhFuncArgs[A: ClassTag, B: ClassTag](args: A => B) extends PhWorkArgs[A => B] {
    /** 获取实际包装的A => B参数
      *
      * @return A => B 返回实际参数的类型
      * @author clock
      * @version 0.1
      * @since 2019/6/11 15:03
      */
    def get: A => B = args
}

/** iPaas Driver 的Nothing参数包装类, 是所有包装类的子类，相当于Nothing
  *
  * @author clock
  * @version 0.1
  * @since 2019/6/11 14:57
  * @note 用法相当于Scala的Nothing或者List的Nil
  */
case object PhNoneArgs extends PhWorkArgs[Nothing] {
    /** PhNoneArgs类无法get，没有意义
      *
      * @return Nothing
      * @throws NoSuchElementException ("PhNoneArgs.get")
      * @author clock
      * @version 0.1
      * @since 2019/6/11 15:03
      */
    def get: Nothing = throw new NoSuchElementException("PhNoneArgs.get")

    /** 判断包装的参数是否为空
      *
      * @return Boolean 未包装任何参数则返回true
      * @author clock
      * @version 0.1
      * @since 2019/6/11 15:05
      */
    override def isEmpty = true
}

