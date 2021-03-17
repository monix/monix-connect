import com.typesafe.tools.mima.core.ProblemFilters.exclude
import com.typesafe.tools.mima.core._

object MimaFilters {

  lazy val changesFor_0_5_3: Seq[ProblemFilter] = Seq(
    exclude[DirectMissingMethodProblem]("monix.connect.s3.domain.package.awsDefaulMaxKeysList")
  )

  lazy val changesFor_0_6_0: Seq[ProblemFilter] = Seq(
    //lettuce breaking changes
    exclude[DirectMissingMethodProblem]("monix.connect.redis.RedisHash.hgetall"),
    exclude[IncompatibleResultTypeProblem]("monix.connect.redis.RedisHash.hgetall"),
    exclude[DirectMissingMethodProblem]("monix.connect.redis.Redis.hgetall"),
    exclude[IncompatibleResultTypeProblem]("monix.connect.redis.Redis.hgetall"),

    exclude[IncompatibleResultTypeProblem]("monix.connect.redis.RedisKey.randomkey")
  )

  val allMimaFilters: Seq[ProblemFilter] = changesFor_0_5_3 ++ changesFor_0_6_0
}
