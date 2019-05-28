package com.pharbers.ipaas.data.driver.api.work

trait pActionArgs extends scala.AnyRef with PhWorkArgs {
    type t
    def get : pActionArgs.this.t
}
