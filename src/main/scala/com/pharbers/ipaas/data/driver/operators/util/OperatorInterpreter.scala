package com.pharbers.ipaas.data.driver.operators.util

import java.io.File
import java.lang.annotation.Annotation
import java.net.URL
import com.pharbers.ipaas.data.driver.api.Annotation.{Operator, Plugin}

/** 功能描述
  * 获得包路径下的所有含有T注解的类路径及注解
  * @author dcs
  * @version 0.0
  * @since 2019/09/16 17:24
  * @note
  */
object OperatorInterpreter {
    /** 功能描述
      * 获取含有Operator注解的类路径和Operator注解
      * @param packageName 包路径
      * @return scala.Seq[(_root_.scala.Predef.String, _root_.com.pharbers.ipaas.data.driver.api.Annotation.Operator)]
      * @author dcs
      * @version 0.0
      * @since 2019/9/17 10:20
      * @note
      * @example
      */
    def getOperatorAnnotations(packageName: String): Seq[(String, Operator)] ={
        OperatorInterpreter.getOperatorClass(packageName, classOf[Operator], true)
    }

    def getPluginAnnotations(packageName: String): Seq[(String, Plugin)] ={
        OperatorInterpreter.getOperatorClass(packageName, classOf[Plugin], true)
    }


    /** 功能描述
      * 获取含有T注解的类路径和注解
      * @param packageName 包路径
      * @param tag 注解类的Class对象
      * @param childPackage 是否迭代搜索包
      * @tparam T 注解类型
      * @return scala.Seq[(_root_.scala.Predef.String, T)]
      * @author dcs
      * @version 0.0
      * @since 2019/9/17 10:21
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    def getOperatorClass[T <: Annotation](packageName: String, tag: Class[T], childPackage: Boolean = false): Seq[(String, T)] = {
        val loader: ClassLoader = Thread.currentThread.getContextClassLoader
        val packagePath: String = packageName.replace(".", "/")
        val url: URL = loader.getResource(packagePath)
        if (url != null) {
            getClassNameByFile(url.getPath, childPackage).map(x => {
                val classPath = x.substring(x.indexOf(packageName))
                (classPath, Class.forName(classPath).getAnnotation(tag))
            }).filter(x => x._2 != null)
        } else {
            Nil
        }
    }

    /**
      * 从项目文件获取某包下所有类
      *
      * @param filePath     文件路径
      * @param childPackage 是否遍历子包
      * @return 类完整路径
      */
    private def getClassNameByFile(filePath: String, childPackage: Boolean): Seq[String] = {
        val file = new File(filePath)
        val childFiles = file.listFiles
        childFiles.filter(x => !x.isDirectory && x.getPath.endsWith(".class"))
                .map(x => x.getPath.substring(x.getPath.indexOf("\\classes") + 9, x.getPath.lastIndexOf(".")).replace("\\", ".")) ++
                childFiles.filter(x => x.isDirectory && childPackage).flatMap(x => getClassNameByFile(x.getPath, childPackage))
    }
}
