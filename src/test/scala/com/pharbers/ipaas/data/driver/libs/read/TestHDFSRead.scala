package com.pharbers.ipaas.data.driver.libs.read

import org.scalatest.FunSuite

/**
 * @author clock
 * @version 0.2
 * @since 2019/08/28 10:46
 * @note
 */
class TestHDFSRead extends FunSuite {
    test("读取HDFS的文件到输入流") {
        val result = HDFSRead("hdfs:///test/MZclean.yaml").toInputStream()
        assert(result != null)
    }
}
