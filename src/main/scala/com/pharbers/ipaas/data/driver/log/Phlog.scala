package com.pharbers.ipaas.data.driver.log

import org.apache.logging.log4j.LogManager

case class Phlog() {
	val logger =LogManager.getLogger()

	def setInfoLog(msg: AnyRef): Unit = {
		logger.info(msg)
	}

	def setTraceLog(msg: AnyRef): Unit = {
		logger.trace(msg)
	}

	def setDebugLog(msg: AnyRef): Unit = {
		logger.debug(msg)
	}

	def setErrorLog(msg: AnyRef): Unit = {
		logger.error(msg)
	}

	def setWarnLog(msg: AnyRef): Unit = {
		logger.warn(msg)
	}

	def setFATALLog(msg: AnyRef): Unit = {
		logger.fatal(msg)
	}
}
