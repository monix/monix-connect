package cloriko.monix.connect.hdfs

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{ Consumer, Observable }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.compress.CompressionCodec

object Hdfs {

  def writer(fs: FileSystem, path: Path)(implicit scheduler: Scheduler): Consumer[Array[Byte], Task[Int]] = {
    new HdfsSubscriber(fs, path)
  }

  def read(fs: FileSystem, path: Path, chunkSize: Int = 8192)(
    implicit scheduler: Scheduler): Observable[Array[Byte]] = {
    Observable.fromInputStream(Task(fs.open(path)))
  }

  def readCompressed(fs: FileSystem, path: Path, codec: CompressionCodec, chunkSize: Int = 8192)(
    implicit scheduler: Scheduler): Observable[Array[Byte]] = {
    Observable.fromInputStream(Task(codec.createInputStream(fs.open(path))))
  }

}
