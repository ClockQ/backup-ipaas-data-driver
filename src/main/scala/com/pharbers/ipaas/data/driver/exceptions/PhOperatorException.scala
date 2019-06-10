package com.pharbers.ipaas.data.driver.exceptions

/** 功能描述
  *
  * @author EDZ
  * @param
  * @tparam
  * @note
  */
case class PhOperatorException(name: String, exception: Exception) extends Exception{
    override def getStackTrace: Array[StackTraceElement] = {
        exception match {
            case _: PhOperatorException => exception.getStackTrace

            case e: Exception => e.getStackTrace

        }
    }

    override def getMessage: String = {
        val message = exception match {
            case e:PhOperatorException => e.getMessage
            case e:Exception => {
                e.toString
            }
        }
        s"$name\n$message"
    }
}
