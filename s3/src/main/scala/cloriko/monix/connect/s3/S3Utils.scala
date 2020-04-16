package cloriko.monix.connect.s3

object S3Utils {

  private val awsPartSizeLimit = 5 * 1024 * 1024

  def divideChunk(byteBuffer: List[Byte]): List[List[Byte]] = {
    val bufferSize = byteBuffer.size
    if (bufferSize > awsPartSizeLimit) {
      val (l1: List[Byte], l2: List[Byte]) = byteBuffer.splitAt(bufferSize / 2)
      (divideChunk(l1) :: divideChunk(l2) :: Nil).flatten
    } else List(byteBuffer)
  }
}
