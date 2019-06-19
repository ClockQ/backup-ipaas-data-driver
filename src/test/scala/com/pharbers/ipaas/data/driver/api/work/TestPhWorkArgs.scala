package com.pharbers.ipaas.data.driver.api.work

import org.scalatest.FunSuite

class TestPhWorkArgs extends FunSuite {
    test("PhWorkArgs") {
        val boolean = PhBooleanArgs(false)
        assert(!boolean.get)
        val string = PhStringArgs("string")
        assert(string.get == "string")
        val list = PhListArgs(boolean :: string :: Nil)
        assert(list.get.length == 2)
        val map = PhMapArgs(Map("key" -> boolean, "key2" -> string))
        assert(map.get.size == 2)
//        val none = PhNoneArgs
//        println(none.get)
        val func = PhFuncArgs((_: Unit) => string)
        assert(func.get(Unit).get == "string")
    }
}
