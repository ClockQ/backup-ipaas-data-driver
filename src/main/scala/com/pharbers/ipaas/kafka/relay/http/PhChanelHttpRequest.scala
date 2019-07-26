package com.pharbers.ipaas.kafka.relay.http
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}

case class PhChanelHttpRequest(url: String, postData: String, headerKey: String = "Content-Type",
                               headerValue: String = "application/json", timeOutMs: Int = 10000
                              ) extends PhhttpRequestTrait {
	override val PhhttpRequest: HttpRequest = Http(url).postData(postData).header(headerKey, headerValue)
		.option(HttpOptions.readTimeout(timeOutMs))
	override def getResponseAsStr: HttpResponse[String] = PhhttpRequest.asString
}
