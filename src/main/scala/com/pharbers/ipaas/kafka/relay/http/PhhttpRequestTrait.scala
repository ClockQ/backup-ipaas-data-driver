package com.pharbers.ipaas.kafka.relay.http

import scalaj.http.{HttpRequest, HttpResponse}

trait PhhttpRequestTrait {
	val PhhttpRequest: HttpRequest
	def getResponseAsStr: HttpResponse[String]
}
