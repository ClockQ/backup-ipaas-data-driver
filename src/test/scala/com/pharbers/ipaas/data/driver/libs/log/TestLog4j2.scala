package com.pharbers.ipaas.data.driver.libs.log

import com.pharbers.util.log.PhLogable
import org.apache.logging.log4j.{LogManager, Logger}
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/09/09 10:49
  * @note 一些值得注意的地方
  */
class TestLog4j2 extends FunSuite {
    test("test log4j config"){
        val logger: Logger = LogManager.getLogger(this)
        logger.debug("test debug")
        logger.trace("test trace")
        logger.warn("test warn")
        logger.error("test error")
        logger.fatal("test fatal")
        logger.info("infoTest")
    }

    test("test logable"){
        import com.pharbers.util.log.PhLogable._
        val logable = new PhLogable {
            def run(): Unit ={
                logger.info("run")
            }
        }
        useJobId("user", "trace", "job")(logable){
            test => test.run()
        }
    }
}
