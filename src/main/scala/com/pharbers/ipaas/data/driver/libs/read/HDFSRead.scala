package com.pharbers.ipaas.data.driver.libs.read

import java.net.URI
import java.io.InputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/** 读取 HDFS 的文件
 *
 * @author clock
 * @version 0.2
 * @since 2019/08/28 10:26
 */
case class HDFSRead(path: String) extends ReadTrait {
    /** 读取 HDFS 文件为输入流 */
    override def toInputStream(): InputStream =
        FileSystem.get(URI.create(path), new Configuration).open(new Path(path)).getWrappedStream
}
