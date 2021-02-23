package monix.connect.redis.domain

import io.lettuce.core.ZAddArgs

object ZArgs extends Enumeration {
  //Only update elements that already exist. Never add elements.
  val XX,
  //Changed elements are new elements added and elements already existing for which the score was updated.
  // So elements specified in the command line having the same score as they had in the past are not counted.
  CH,
  // Don't update already existing elements. Always add new elements.
  NX = Value
  type ZArg = Value

  def parse(zArg: ZArg): ZAddArgs = {
    zArg match {
      case ZArgs.XX => new ZAddArgs().xx()
      case ZArgs.CH => new ZAddArgs().ch()
      case ZArgs.NX => new ZAddArgs().nx()
    }
  }
}

