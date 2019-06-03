package com.pharbers.ipaas.data.driver.config

import java.io.InputStream
import scala.reflect.ClassTag

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
trait ConfigReaderTrait {
    def readObjects[T: ClassTag] (stream: InputStream): Seq[T]

    def readObject[T: ClassTag] (stream: InputStream): T
}
