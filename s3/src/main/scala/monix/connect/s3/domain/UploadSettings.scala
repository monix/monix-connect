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

import software.amazon.awssdk.services.s3.model.{ObjectCannedACL, RequestPayer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

/**
  * @param grantFullControl        Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
  * @param grantRead               Allows grantee to read the object data and its metadata.
  * @param grantReadACP            Allows grantee to read the object ACL.
  * @param grantWriteACP           Allows grantee to write the ACL for the applicable object.
  * @param serverSideEncryption    The server-side encryption algorithm used when storing this object in Amazon S3 (for example, AES256, aws:kms).
  * @param sseCustomerAlgorithm    Specifies the algorithm to use to when encrypting the object (for example, AES256).
  * @param sseCustomerKey          Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data.
  * @param sseCustomerKeyMD5       Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321.
  * @param ssekmsEncryptionContext Specifies the AWS KMS Encryption Context to use for object encryption.
  * @param ssekmsKeyId             Specifies the ID of the symmetric customer managed AWS KMS CMK to use for object encryption.
  * @param requestPayer            Returns the value of the RequestPayer property for this object.
  */
case class UploadSettings(
  acl: Option[ObjectCannedACL] = None,
  //contentType: Option[String] = None,
  grantFullControl: Option[String] = None,
  grantRead: Option[String] = None,
  grantReadACP: Option[String] = None,
  grantWriteACP: Option[String] = None,
  serverSideEncryption: Option[String] = None,
  sseCustomerAlgorithm: Option[String] = None,
  sseCustomerKey: Option[String] = None,
  sseCustomerKeyMD5: Option[String] = None,
  ssekmsEncryptionContext: Option[String] = None,
  ssekmsKeyId: Option[String] = None,
  requestPayer: Option[RequestPayer] = None,
  lastUploadTimeout: FiniteDuration = 1.minute)
