/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.s3.domain

import java.time.Instant

import software.amazon.awssdk.services.s3.model.RequestPayer

/**
  * @param ifMatch              Return the object only if its entity tag (ETag) is the same as the one specified, otherwise return a 412
  * @param ifModifiedSince      Return the object only if it has been modified since the specified time, otherwise return a 304 (not
  *                             modified).
  * @param ifNoneMatch          Return the object only if its entity tag (ETag) is different from the one specified, otherwise return a 304
  *                             (not modified).
  * @param ifUnmodifiedSince    Return the object only if it has not been modified since the specified time, otherwise return a 412
  *                             (precondition failed).
  * @param versionId            VersionId used to reference a specific version of the object.
  * @param sseCustomerAlgorithm Specifies the algorithm to use to when encrypting the object (for example, AES256).
  * @param sseCustomerKey       Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to
  *                             store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be
  *                             appropriate for use with the algorithm specified in the
  *                             <code>x-amz-server-side​-encryption​-customer-algorithm</code> header.
  * @param sseCustomerKeyMD5    Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for
  *                             a message integrity check to ensure that the encryption key was transmitted without error.
  * @param requestPayer         Sets the value of the RequestPayer property for this object.
  */
case class DownloadSettings(
  ifMatch: Option[String] = None,
  ifModifiedSince: Option[Instant] = None,
  ifNoneMatch: Option[String] = None,
  ifUnmodifiedSince: Option[Instant] = None,
  requestPayer: Option[RequestPayer] = None,
  sseCustomerAlgorithm: Option[String] = None,
  sseCustomerKey: Option[String] = None,
  sseCustomerKeyMD5: Option[String] = None,
  versionId: Option[String] = None)
//contemplate part number
