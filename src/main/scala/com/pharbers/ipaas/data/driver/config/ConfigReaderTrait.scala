package com.pharbers.ipaas.data.driver.config

import java.io.InputStream
import scala.reflect.ClassTag

/** 功能描述
  *读取配置文件接口
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 15:27
  * @note
  */
trait ConfigReaderTrait {
    /** 功能描述
      *
     读取为对象集合
      * @param stream 配置文件输入流
      * @tparam T 返回的对象类型
      * @return _root_.scala.reflect.ClassTag[T] => scala.Seq[T]
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 15:36
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    def readObjects[T: ClassTag] (stream: InputStream): Seq[T]

    /** 功能描述
      *
     读取为单个对象
      * @param stream 配置文件输入流
      * @tparam T 返回的对象类型
      * @return _root_.scala.reflect.ClassTag[T] => T
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 15:36
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    def readObject[T: ClassTag] (stream: InputStream): T
}
