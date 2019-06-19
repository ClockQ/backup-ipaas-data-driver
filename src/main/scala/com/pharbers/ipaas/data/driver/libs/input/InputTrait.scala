package com.pharbers.ipaas.data.driver.libs.input

import java.io.InputStream
import scala.reflect.ClassTag

/** 读取输入文件接口
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/11 15:27
  * @note
  */
trait InputTrait {

    /** 读取为单个对象 T
      *
      * @param stream 配置文件输入流
      * @tparam T 返回的对象类型
      * @return _root_.scala.reflect.ClassTag[T] => T
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 15:36
      */
    def readObject[T: ClassTag](stream: InputStream): T

    /** 读取为对象集合 Seq[T]
      *
      * @param stream 配置文件输入流
      * @tparam T 返回的对象类型
      * @return _root_.scala.reflect.ClassTag[T] => scala.Seq[T]
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 15:36
      */
    def readObjects[T: ClassTag](stream: InputStream): Seq[T]
}
