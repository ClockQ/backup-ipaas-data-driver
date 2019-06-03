package com.pharbers.ipaas.data.driver.log

import org.apache.logging.log4j.LogManager

trait Phlog {
	def setInfoLog(msg: String): Unit = {
		LogManager.getLogger().info(msg)
	}

	def setTraceLog(msg: String): Unit = {
		LogManager.getLogger().trace(msg)
	}

	def setDebugLog(msg: String): Unit = {
		LogManager.getLogger().trace(msg)
	}

	def setErrorLog(msg: String): Unit = {
		LogManager.getLogger().trace(msg)
	}
	def setWarnLog(msg: String): Unit = {
		LogManager.getLogger().trace(msg)
	}

	def setFATALLog(msg: String): Unit = {
		LogManager.getLogger().trace(msg)
	}
}
