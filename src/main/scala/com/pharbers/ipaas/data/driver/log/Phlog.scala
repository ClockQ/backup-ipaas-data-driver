package com.pharbers.ipaas.data.driver.log

import org.apache.logging.log4j.LogManager

case class Phlog() {
	val logger = LogManager.getLogger()

	def setInfoLog(msg: String): Unit = {
		logger.info(msg)
	}

	def setTraceLog(msg: String): Unit = {
		logger.trace(msg)
	}

	def setDebugLog(msg: String): Unit = {
		logger.debug(msg)
	}

	def setErrorLog(msg: String): Unit = {
		logger.error(msg)
	}

	def setWarnLog(msg: String): Unit = {
		logger.warn(msg)
	}

	def setFATALLog(msg: String): Unit = {
		logger.fatal(msg)
	}
}
