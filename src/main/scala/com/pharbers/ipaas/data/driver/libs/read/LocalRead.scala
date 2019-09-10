package com.pharbers.ipaas.data.driver.libs.read

import java.io.{File, FileInputStream, InputStream}

/** 读取本地的文件
 *
 * @author clock
 * @version 0.2
 * @since 2019/08/28 10:26
 */
case class LocalRead(path: String) extends ReadTrait {
    /** 读取本地文件为输入流 */
    override def toInputStream(): InputStream = new FileInputStream(new File(path))
}
