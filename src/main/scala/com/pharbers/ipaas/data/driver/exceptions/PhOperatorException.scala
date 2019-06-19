package com.pharbers.ipaas.data.driver.exceptions

/** 捕获job运行异常及job->action->operator链
  *
  * @author dcs
  * @version 0.1
  * @since 2019/6/11 15:27
  * @note
  */
case class PhOperatorException(names: Seq[String], exception: Exception) extends Exception {
    /** 获取异常StackTrace
      *
      * @return _root_.scala.Array[_root_.java.lang.StackTraceElement]
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 15:54
      */
    override def getStackTrace: Array[StackTraceElement] =
        exception.getStackTrace

    /** 获取异常描述
      *
      * @return _root_.scala.Predef.String
      * @author dcs
      * @version 0.1
      * @since 2019/6/11 15:55
      */
    override def getMessage: String =
        (names ++ exception.getStackTrace.map(x => x.toString)).mkString("\n")
}
