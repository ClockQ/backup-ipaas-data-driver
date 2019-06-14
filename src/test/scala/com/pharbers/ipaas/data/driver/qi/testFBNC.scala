package com.pharbers.ipaas.data.driver.qi

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author clock
  * @version 0.0
  * @since 2019/06/12 10:00
  * @note 一些值得注意的地方
  */
object testFBNC extends App {
    // 普通递归，效率最低
//    def fib(n: Int): Int = {
//        if(n == 0) 0
//        else if(n == 1) 1
//        else fib(n-1) + fib(n-2)
//    }
    // 尾递归
//    def fib(n: Long): Long = {
//        def loop(n: Long, a: Long, b: Long): Long = {
//            if(n == 0) a
//            else loop(n-1, b, a + b)
//        }
//
//        loop(n, 0, 1)
//    }
//
//    println(fib(0))
//    println(fib(1))
//    println(fib(2))
//    println(fib(3))
//    println(fib(4))
//    println(fib(5))
//    println(fib(100))
//
//    def isSorted[A](as: Array[A], ordered: (A, A) => Boolean): Boolean = {
//        def loop(): Boolean {
//
//        }
//    }
//
//    println(isSorted(Array(10, 4, 3, 2, 8 ,4, 5, 1, 2, 5, 9), ???))
//
//    def curry[A, B, C](f: (A, B) => C): A => B => C = { a: A => (b: B) => f(a, b) }
//
//    def uncurry[A, B, C](f: A => B => C): (A, B) => C = { (a: A, b: B) =>f(a)(b) }
//
//    def compose[A, B, C](f: B => C, g: A => B) = (a: A) =>f(g(a))

//    def length[A](lst: List[A]): Int = {
//        lst.foldRight(0)((_, b) => 1 + b)
//    }
//    println(length(List(1, 2, 3, 4, 5, 6, 7, 8, 9)))

//    @tailrec
//    def foldLeft[A, B](as: List[A], z: B)(f: (B, A) => B): B = {
//        as match {
//            case Nil => z
//            case head :: tail => foldLeft(tail, f(z, head))(f)
//        }
//    }
//
//    println(foldLeft(List(1, 2, 3, 4, 5), 0)(_ + _))

    def reverse[A](as: List[A]): List[A] = {
//        as.foldRight(Nil: List[A])((l, r) => r ::: l :: Nil)
        as.foldLeft(Nil: List[A])((l, r) => r :: l)
    }

    println(reverse(List(1, 2, 3, 4, 5)))

}
