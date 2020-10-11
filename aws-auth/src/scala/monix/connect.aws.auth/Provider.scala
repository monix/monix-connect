package monix.connect.aws.auth

object Provider extends Enumeration {
  type Type = Value
  val Anonymous, Chain, Default, Environment, Static = Value

  def fromString(str: String) = {
    str.toLowerCase match {
      case "anonymous" => Anonymous
      case "chain" => Chain
      case "default" => Default
      case "environment" => Environment
      case "static" => Static
      case _ => Default
    }
  }
}