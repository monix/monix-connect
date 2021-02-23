package monix.connect.redis.domain

import io.lettuce.core.{Range => R}

final class ZRange[T](private[redis] val underlying: R[T])

object ZRange {

  def apply[T](lower: T, upper: T): ZRange[T] = new ZRange(R.create(lower, upper))

  def create[T](lower: T, upper: T): ZRange[T] = new ZRange(R.create(lower, upper))

  def unbounded[T](): ZRange[T] = new ZRange(R.unbounded())

  def lt[T](upperLimit: T): ZRange[T] = new ZRange(R.from(R.Boundary.unbounded(), R.Boundary.excluding(upperLimit)))

  def lte[T](upperLimit: T): ZRange[T] = new ZRange(R.from(R.Boundary.unbounded(), R.Boundary.including(upperLimit)))

  def gt[T](lowerLimit: T): ZRange[T] = new ZRange(R.from(R.Boundary.excluding(lowerLimit), R.Boundary.unbounded()))

  def gte[T](lowerLimit: T): ZRange[T] = new ZRange(R.from(R.Boundary.including(lowerLimit), R.Boundary.unbounded()))

}
