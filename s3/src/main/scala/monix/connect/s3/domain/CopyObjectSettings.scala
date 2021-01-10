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

package monix.connect.s3.domain

import java.time.Instant

import software.amazon.awssdk.services.s3.model.{
  MetadataDirective,
  ObjectCannedACL,
  ObjectLockLegalHoldStatus,
  ObjectLockMode,
  RequestPayer,
  ServerSideEncryption,
  StorageClass,
  TaggingDirective
}

/**
  * @param copySourceIfMatches   copies the object if its entity tag (ETag) matches the specified tag.
  * @param copySourceIfNoneMatch copies the object if its entity tag (ETag) is different than the specified ETag.
  * @param copyIfModifiedSince   copies the object if it has been modified since the specified time.
  * @param copyIfUnmodifiedSince copies the object if it hasn't been modified since the specified time.
  * @param expires               the date and time at which the object is no longer cacheable.
  * @param acl                   the canned ACL to apply to the object.
  * @param grantFullControl      gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
  * @param grantRead             allows grantee to read the object data and its metadata.
  * @param grantReadACP          allows grantee to read the object ACL.
  * @param grantWriteACP         allows grantee to write the ACL for the applicable object.
  * @param metadata              a map of metadata to store with the object in S3.
  * @param metadataDirective     a map of metadata to store with the object in S3.
  * @param taggingDirective      specifies whether the object tag-set are copied from the source object or replaced with tag-set provided in the
  *                              request.
  * @param serverSideEncryption  specifies the server-side encryption algorithm used when storing this object in Amazon S3 (for example, AES256, aws:kms).
  * @param storageClass          specifies the type of storage to use for the object. Defaults to 'STANDARD'.
  * @param sseCustomerAlgorithm  Specifies the algorithm to use to when encrypting the object (for example, AES256).
  * @param sseCustomerKey        specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. This value is used to
  *                              store the object and then it is discarded; Amazon S3 does not store the encryption key. The key must be
  *                              appropriate for use with the algorithm specified in the
  * @param sseCustomerKeyMD5     specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a
  *                              message integrity check to ensure that the encryption key was transmitted without error.
  * @param ssekmsKeyId           specifies the AWS KMS key ID to use for object encryption. All GET and PUT requests for an object protected by
  *                              AWS KMS will fail if not made via SSL or using SigV4. For information about configuring using any of the
  *                              officially supported AWS SDKs and AWS CLI.
  *                              href="https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version">
  * @param copySourceSSECustomerAlgorithm the algorithm to use when decrypting the source object (for example, AES256).
  * @param copySourceSSECustomerKey    specifies the customer-provided encryption key for Amazon S3 to use to decrypt the source object. The encryption
  *                                    key provided in this header must be one that was used when the source object was created.
  * @param copySourceSSECustomerKeyMD5 specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. Amazon S3 uses this header for a
  *                                    message integrity check to ensure that the encryption key was transmitted without error.
  * @param objectLockMode              the Object Lock mode that you want to apply to the copied object.
  * @param objectLockRetainUntilDate   the date and time when you want the copied object's Object Lock to expire
  * @param objectLockLegalHoldStatus   specifies whether you want to apply a Legal Hold to the copied object.
  * @param requestPayer                confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this
  *                                    parameter in their requests.
  */
case class CopyObjectSettings(
  copySourceIfMatches: Option[String] = None,
  copySourceIfNoneMatch: Option[String] = None,
  copyIfModifiedSince: Option[Instant] = None,
  copyIfUnmodifiedSince: Option[Instant] = None,
  expires: Option[Instant] = None,
  acl: Option[ObjectCannedACL] = None,
  grantFullControl: Option[String] = None,
  grantRead: Option[String] = None,
  grantReadACP: Option[String] = None,
  grantWriteACP: Option[String] = None,
  metadata: Map[String, String] = Map.empty,
  metadataDirective: Option[MetadataDirective] = None,
  taggingDirective: Option[TaggingDirective] = None,
  serverSideEncryption: Option[ServerSideEncryption] = None,
  storageClass: StorageClass = StorageClass.STANDARD,
  sseCustomerAlgorithm: Option[String] = None,
  sseCustomerKey: Option[String] = None,
  sseCustomerKeyMD5: Option[String] = None,
  ssekmsKeyId: Option[String] = None,
  copySourceSSECustomerAlgorithm: Option[String] = None,
  copySourceSSECustomerKey: Option[String] = None,
  copySourceSSECustomerKeyMD5: Option[String] = None,
  objectLockMode: Option[ObjectLockMode] = None,
  objectLockRetainUntilDate: Option[Instant] = None,
  objectLockLegalHoldStatus: Option[ObjectLockLegalHoldStatus] = None,
  requestPayer: Option[RequestPayer] = None)
