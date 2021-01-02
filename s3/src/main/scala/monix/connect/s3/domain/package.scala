/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.s3

package object domain {

  /**
    * The minimum allowable part size for a multipart upload is 5 MB.
    * @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    * For more information about multipart upload limits.
    */
  val awsMinChunkSize: Int = 5 * 1024 * 1024 //5242880 bytes
  val awsDefaulMaxKeysList = 1000 // represents the default max keys request

  //default setting instances
  private[s3] val DefaultDownloadSettings = DownloadSettings()
  private[s3] val DefaultCopyObjectSettings = CopyObjectSettings()
  private[s3] val DefaultUploadSettings = UploadSettings()

}
