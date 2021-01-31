import com.typesafe.tools.mima.core.ProblemFilters.exclude
import com.typesafe.tools.mima.core._

object MimaFilters {

  lazy val changesFor_0_5_3: Seq[ProblemFilter] = Seq(
    exclude[DirectMissingMethodProblem]("monix.connect.s3.domain.package.awsDefaulMaxKeysList") ,
    exclude[DirectMissingMethodProblem]("monix.connect.s3.S3.deleteBucket"),
    exclude[DirectMissingMethodProblem]("monix.connect.s3.S3.deleteObject")
  )
}
