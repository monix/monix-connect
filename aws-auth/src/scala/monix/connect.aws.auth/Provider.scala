package monix.connect.aws.auth

import monix.execution.internal.InternalApi

@InternalApi
private[connect] object Provider extends Enumeration {
  type Type = Value
  val Anonymous, Chain, Default, Environment, Instance, System, Profile, Static = Value

  def fromString(str: String): Provider.Value = {
    str.toLowerCase match {
      case "anonymous" => Anonymous
      case "chain" => Chain
      case "default" => Default
      case "environment" => Environment
      case "instance" => Instance
      case "profile" => Profile
      case "static" => Static
      case "system" => System
      case _ => Default
    }
  }
}