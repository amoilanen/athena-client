package io.github.antivanov.athena

import java.io.{File, InputStreamReader}
import java.util

import org.scalatest.{FreeSpec, Matchers}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, ListBucketsRequest, ListObjectsRequest, PutObjectRequest, S3Object}

import scala.io.Source
import scala.jdk.CollectionConverters._

class AthenaClientItSpec extends FreeSpec with Matchers {

  val outputBucketName = "athena-it-tests-26449690-45c1-46e8-aff9-235e8cced1b2"
  val region = Region.EU_CENTRAL_1
  val s3Client = S3Client.builder.region(region).credentialsProvider(DefaultCredentialsProvider.create).build;

  def readResource(fileName: String): String =
    Source.fromResource(fileName).getLines().toList.mkString("\n")

  def existingBuckets(): Seq[String] = {
    val request = ListBucketsRequest.builder.build
    val response = s3Client.listBuckets(request)
    response.buckets.asScala.toSeq.map(_.name)
  }

  def bucketExists(bucketName: String): Boolean = {
    existingBuckets().contains(bucketName)
  }

  def createBucket(bucketName: String): Unit= {
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
  }

  def putObject(bucketName: String, key: String, contents: String): Unit = {
    s3Client.putObject(
      PutObjectRequest.builder().bucket(bucketName).key(key).build(),
      RequestBody.fromString(contents)
    )
  }

  def deleteBucket(bucketName: String): Unit = {
    val objectsInBucket: List[S3Object] = s3Client.listObjects(ListObjectsRequest.builder().bucket(bucketName).build())
      .contents().asScala.toList
    objectsInBucket.foreach(obj =>
      s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(obj.key).build())
    )

    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
  }

  "AthenaClient" - {

    "should be able to execute a query" - {
      if (!bucketExists(outputBucketName)) {
        createBucket(outputBucketName)
      }
      putObject(outputBucketName, "cities.csv", readResource("cities.csv"))

      deleteBucket(outputBucketName)
    }
  }
}
