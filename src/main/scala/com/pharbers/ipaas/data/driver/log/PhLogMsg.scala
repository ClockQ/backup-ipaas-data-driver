package com.pharbers.ipaas.data.driver.log

case class PhLogMsg(
                   user: String,
                   traceID: String,
                   jobID: String,
                   clazz: String,
                   description: String
                   ) {
    override def toString: String = s"$clazz : $description, user:$user , traceID:$traceID, jobID: $jobID"
}
