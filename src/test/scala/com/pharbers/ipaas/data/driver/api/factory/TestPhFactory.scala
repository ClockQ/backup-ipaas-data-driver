package com.pharbers.ipaas.data.driver.api.factory

import java.util

import com.pharbers.ipaas.data.driver.api.model._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters.mapAsJavaMapConverter

class TestPhFactory extends FunSuite with BeforeAndAfterAll {
    var sub: Plugin = _
    var plugin: Plugin = _
    var operator: Operator = _
    var action: Action = _
    var job: Job = _

    override def beforeAll(): Unit = {
        sub = Plugin()
        sub.setName("sub")
        sub.setFactory("com.pharbers.ipaas.data.driver.api.factory.PhPluginFactory")
        sub.setReference("com.pharbers.ipaas.data.driver.plugins.ExprPlugin")
        sub.setArgs(Map("exprString" -> "exprString").asJava)
        require(sub != null)

        plugin = Plugin()
        plugin.setName("plugin")
        plugin.setFactory("com.pharbers.ipaas.data.driver.api.factory.PhPluginFactory")
        plugin.setReference("com.pharbers.ipaas.data.driver.plugins.ExprPlugin")
        plugin.setArgs(Map("exprString" -> "exprString").asJava)
        plugin.setSub(sub)
        require(plugin != null)

        operator = Operator()
        operator.setName("operator")
        operator.setFactory("com.pharbers.ipaas.data.driver.api.factory.PhOperatorFactory")
        operator.setReference("com.pharbers.ipaas.data.driver.operators.AddColumnOperator")
        operator.setArgs(Map("inDFName" -> "inDFName", "newColName" -> "newColName").asJava)
        operator.setPlugin(plugin)
        require(operator != null)

        action = Action()
        action.setName("action")
        action.setFactory("com.pharbers.ipaas.data.driver.api.factory.PhActionFactory")
        action.setReference("com.pharbers.ipaas.data.driver.api.job.PhBaseAction")
        action.setOpers {
            val tmp = new java.util.ArrayList[Operator]
            tmp.add(operator)
            tmp
        }
        require(action != null)

        job = Job()
        job.setName("job")
        job.setFactory("com.pharbers.ipaas.data.driver.api.factory.PhJobFactory")
        job.setReference("com.pharbers.ipaas.data.driver.api.job.PhBaseJob")
        job.setActions{
            val tmp = new util.ArrayList[Action]
            tmp.add(action)
            tmp
        }
        require(job != null)
    }

    def afterAll(configMap: Map[String, Any]): Unit = {
        Unit
    }

    test("PhPluginFactory") {
        val inst = PhPluginFactory(plugin).inst()

        assert("plugin" == inst.name)
        assert("sub" == inst.subPluginLst.head.name)
    }

    test("PhOperatorFactory") {
        val inst = PhOperatorFactory(operator).inst()

        assert("operator" == inst.name)
        assert("plugin" == inst.pluginLst.head.name)
    }

    test("PhActionFactory") {
        val inst = PhActionFactory(action).inst()

        assert("action" == inst.name)
        assert("operator" == inst.operatorLst.head.name)
        assert("plugin" == inst.operatorLst.head.pluginLst.head.name)
    }

    test("PhJobFactory") {
        val inst = PhJobFactory(job).inst()

        assert("job" == inst.name)
        assert("action" == inst.actionLst.head.name)
        assert("operator" == inst.actionLst.head.operatorLst.head.name)
        assert("plugin" == inst.actionLst.head.operatorLst.head.pluginLst.head.name)
    }
}
