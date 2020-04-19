package cloriko.monix.connect.hdfs

import org.scalacheck.Gen

trait HdfsFixture {
  val genFileName: Gen[String] = Gen.alphaLowerStr
  val genChunk: Gen[Array[Byte]] = Gen.alphaLowerStr.map(_.getBytes)
  val genChunks: Gen[List[Array[Byte]]] = Gen.listOfN(10, genChunk)
}
