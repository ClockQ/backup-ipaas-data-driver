package com.pharbers.ipaas.data.driver.libs.read

import java.io.InputStream

/** 将指定内容转为输入流接口
 *
 * @author clock
 * @version 0.2
 * @since 2019/08/28 10:22
 */
trait ReadTrait {
    /** 输出为输入流
     *
     * @return _root_.java.io.InputStream
     * @author clock
     * @version 0.2
     * @since 2019/8/28 10:26
     */
    def toInputStream(): InputStream
}
