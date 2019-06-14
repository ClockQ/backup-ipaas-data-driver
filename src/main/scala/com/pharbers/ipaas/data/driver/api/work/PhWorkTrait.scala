package com.pharbers.ipaas.data.driver.api.work

sealed trait PhWorkTrait extends Serializable {
    val name: String
    val defaultArgs: PhWorkArgs[_]

    def perform(pr: PhWorkArgs[_]): PhWorkArgs[_]
}

trait PhPluginTrait extends PhWorkTrait

trait PhOperatorTrait extends PhWorkTrait

trait PhActionTrait extends PhWorkTrait {
    val operatorLst: List[PhOperatorTrait]
}

trait PhJobTrait extends PhWorkTrait {
    val actionLst: List[PhActionTrait]
}

sealed trait PhWorkTrait2[+A] extends Serializable {
    val name: String
    val defaultArgs: PhMapArgs[PhWorkArgs[Any]]

    def perform(pr: PhMapArgs[PhWorkArgs[Any]]): PhWorkArgs[A]
}

trait PhPluginTrait2[+A] extends PhWorkTrait2[A] {
    val subPluginLst: Seq[PhPluginTrait2[Any]]
}

trait PhOperatorTrait2[+A] extends PhWorkTrait2[A] {
    val pluginLst: Seq[PhPluginTrait2[Any]]
}

trait PhActionTrait2 extends PhWorkTrait2[Any] {
    val operatorLst: Seq[PhOperatorTrait2[Any]]
}

trait PhJobTrait2 extends PhWorkTrait2[Any] {
    val actionLst: Seq[PhActionTrait2]
}