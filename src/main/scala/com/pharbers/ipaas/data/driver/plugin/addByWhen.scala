package com.pharbers.ipaas.data.driver.plugin

import com.pharbers.ipaas.data.driver.api.work.{PhColArgs, PhMapArgs, PhNoneArgs, PhOperatorTrait, PhWorkArgs}
import org.apache.spark.sql.functions._

/** Adds a key/value pair to this map, returning a new map.
  *  @param    kv the key/value pair
  *  @tparam   B1 the type of the value in the key/value pair.
  *  @return   a new map with the new binding added to this map
  *
  *  @example  def + (kv: (A, B)): Map[A, B]
  */
case class addByWhen[T <: Map[String, PhWorkArgs[_]]](args: PhWorkArgs[T]) extends PhOperatorTrait{
    override val name: String = "add column by when"
    override val defaultArgs: PhWorkArgs[_] = PhNoneArgs

	/** Adds a key/value pair to this map, returning a new map.
	  *  @param    pr the key/value pair
	  *  @return   a new map with the new binding added to this map
	  *
	  *  @example  def + (kv: (A, B)): Map[A, B]
	  */
    override def perform(pr: PhWorkArgs[_]): PhWorkArgs[_] = {
        val condition = args.get.getOrElse("condition",throw new Exception("not found condition")).get.asInstanceOf[String]
        val trueValue = args.get.getOrElse("trueValue",throw new Exception("not found trueValue")).get
        val otherValue = args.get.getOrElse("otherValue",throw new Exception("not found otherValue")).get

        PhColArgs(when(expr(condition), trueValue).otherwise(otherValue))
    }
}
