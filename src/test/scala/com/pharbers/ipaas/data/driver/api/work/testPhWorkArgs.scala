package com.pharbers.ipaas.data.driver.api.work

/**
  * @description:
  * @author: clock
  * @date: 2019-05-28 15:06
  */
object testPhWorkArgs extends App {
    val boolean = PhBooleanArgs(false)
    println(boolean.get)
    val string = PhStringArgs("string")
    println(string.get)
    val list = PhListArgs(boolean :: string :: Nil)
    println(list.get)
    val map = PhMapArgs(Map("key" -> boolean, "key2" -> string))
    println(map.get)
    val none = PhNoneArgs
    println(none.get)
    val func = PhFuncArgs(_ => string)
    println(func.get)
    println(func.get(boolean))
}
