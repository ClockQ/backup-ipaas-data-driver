package com.pharbers.ipaas.data.driver.api.work

import com.pharbers.ipaas.data.driver.libs.log.PhLogDriver

/** iPaas Driver 的 PhLogDriver 参数包装类
 *
 * @param args 实际包装的 PhLogDriver 参数，没有默认值
 * @author clock
 * @version 0.1
 * @since 2019/6/18 17:31
 * @note 包装的运行环境
 */
final case class PhLogDriverArgs(args: PhLogDriver) extends PhWorkArgs[PhLogDriver] {
    /** 获取实际包装的 PhLogDriver 参数
     *
     * @return PhSparkDriver 返回包装的 PhLogDriver
     * @author clock
     * @version 0.1
     * @since 2019/6/18 17:34
     */
    def get: PhLogDriver = args

    override def isEmpty = true
}
