package com.pharbers.ipaas.data.driver.libs.read

import java.io.{ByteArrayInputStream, InputStream}

/** 读取 String 到 Stream
 *
 * @author clock
 * @version 0.2
 * @since 2019/08/28 10:26
 */
case class StringRead(data: String) extends ReadTrait {
    /** 读取字符串转为输入流 */
    override def toInputStream(): InputStream = new ByteArrayInputStream(data.getBytes)
}
