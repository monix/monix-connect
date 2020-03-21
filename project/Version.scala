import java.io.File
import java.nio.charset.Charset
import sbt.IO

object Version {
  val version = IO.readLines(new File("VERSION"), Charset.forName("UTF-8")).head
}
