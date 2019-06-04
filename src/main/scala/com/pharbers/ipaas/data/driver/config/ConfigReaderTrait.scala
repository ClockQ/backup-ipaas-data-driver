package com.pharbers.ipaas.data.driver.config

import java.io.InputStream
import scala.reflect.ClassTag

/** 读取配置文件接口
  * @author dcs
  * @tparam T
  * @note
  */
trait ConfigReaderTrait {
    def readObjects[T: ClassTag] (stream: InputStream): Seq[T]

    def readObject[T: ClassTag] (stream: InputStream): T
}
