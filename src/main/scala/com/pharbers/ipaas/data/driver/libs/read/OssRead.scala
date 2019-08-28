package com.pharbers.ipaas.data.driver.libs.read

import java.io.InputStream
import com.aliyun.oss.{OSS, OSSClientBuilder}

/** 读取 OSS 的文件
 *
 * @author clock
 * @version 0.2
 * @since 2019/08/28 10:26
 */
object OssRead {
    // TODO: 不安全，配置要提出去
    val endpoint = "oss-cn-beijing.aliyuncs.com"
    val accessKeyId = "LTAIEoXgk4DOHDGi"
    val accessKeySecret = "x75sK6191dPGiu9wBMtKE6YcBBh8EI"
    val bucketName = "pharbers-resources"

    val client: OSS = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret)
}

case class OssRead(path: String) extends ReadTrait {

    /** 读取 OSS 文件为输入流 */
    override def toInputStream(): InputStream = {
        val ossObj = OssRead.client.getObject(OssRead.bucketName, path)
        ossObj.getObjectContent
    }
}
