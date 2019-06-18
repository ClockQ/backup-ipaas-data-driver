package com.pharbers.ipaas.data.driver.exceptions

/** 功能描述
  *捕获job运行异常及job->action->operator链
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 15:27
  * @note
  */
case class PhOperatorException(names: Seq[String], exception: Exception) extends Exception{
    /** 功能描述
      *
     获取异常StackTrace
      * @return _root_.scala.Array[_root_.java.lang.StackTraceElement]
      * @author EDZ
      * @version 0.0
      * @since 2019/6/11 15:54
      * @note 一些值得注意的地方
      * @example {{{这是一个例子}}}
      */
    override def getStackTrace: Array[StackTraceElement] = {
        exception.getStackTrace
    }
/** 功能描述
  *
 获取异常描述
  * @return _root_.scala.Predef.String
  * @author EDZ
  * @version 0.0
  * @since 2019/6/11 15:55
  * @note 一些值得注意的地方
  * @example {{{这是一个例子}}}
  */
    override def getMessage: String = {
        (names ++ List(exception.toString + "\n") ++ exception.getStackTrace.map(x => x.toString)).mkString("\n")
    }
}
