package com.pharbers.ipaas.data.driver.api.work

import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver

/** iPaas Driver 的 PhSparkDriver 参数包装类
 *
 * @param args 实际包装的 PhSparkDriver 参数，没有默认值
 * @author clock
 * @version 0.1
 * @since 2019/6/15 17:31
 * @note 包装的运行环境
 */
final case class PhSparkDriverArgs(args: PhSparkDriver) extends PhWorkArgs[PhSparkDriver] {
    /** 获取实际包装的 PhSparkDriver 参数
     *
     * @return PhSparkDriver 返回包装的 PhSparkDriver
     * @author clock
     * @version 0.1
     * @since 2019/6/15 17:34
     */
    def get: PhSparkDriver = args

    override def isEmpty = true
}
