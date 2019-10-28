package io.github.antivanov.athena.util

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.{S3Client => AWSS3Client}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, ListBucketsRequest, ListObjectsRequest, PutObjectRequest, S3Object}

import scala.jdk.CollectionConverters._

case class S3Client(region: Region) {

  val s3: AWSS3Client = AWSS3Client.builder.region(region).credentialsProvider(DefaultCredentialsProvider.create).build;

  def existingBuckets(): Seq[String] = {
    val request = ListBucketsRequest.builder.build
    val response = s3.listBuckets(request)
    response.buckets.asScala.toSeq.map(_.name)
  }

  def ensureExists(bucketName: String): Unit = {
    if (!bucketExists(bucketName)) {
      createBucket(bucketName)
    }
  }

  def bucketExists(bucketName: String): Boolean = {
    existingBuckets().contains(bucketName)
  }

  def createBucket(bucketName: String): Unit = {
    s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
  }

  def putObject(bucketName: String, key: String, contents: String): Unit = {
    s3.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(key).build(),
      RequestBody.fromString(contents)
    )
  }

  def deleteBucket(bucketName: String): Unit = {
    val objectsInBucket: List[S3Object] = s3.listObjects(ListObjectsRequest.builder().bucket(bucketName).build())
      .contents().asScala.toList
    objectsInBucket.foreach(obj =>
      s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(obj.key).build())
    )

    s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
  }
}
