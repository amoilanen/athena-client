package io.github.antivanov.athena

import org.scalatest.{FreeSpec, Matchers}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, DeleteBucketRequest, ListBucketsRequest}

import scala.jdk.CollectionConverters._

class AthenaClientItSpec extends FreeSpec with Matchers {

  val outputBucketName = "athena-it-tests-26449690-45c1-46e8-aff9-235e8cced1b2"
  val region = Region.EU_CENTRAL_1
  val s3Client = S3Client.builder.region(region).credentialsProvider(DefaultCredentialsProvider.create).build;

  def existingBuckets(): Seq[String] = {
    val request = ListBucketsRequest.builder.build
    val response = s3Client.listBuckets(request)
    response.buckets.asScala.toSeq.map(_.name)
  }

  def bucketExists(bucketName: String): Boolean = {
    println(existingBuckets())
    existingBuckets().contains(bucketName)
  }

  def createBucket(bucketName: String): Unit= {
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
  }

  def deleteBucket(bucketName: String): Unit = {
    //TODO: Also delete all the files contained in the bucket
    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
  }

  "AthenaClient" - {

    "should be able to execute a query" - {
      if (!bucketExists(outputBucketName)) {
        createBucket(outputBucketName)
      }
      //TODO: Upload a CSV file with the Athena table data to the S3 bucket
      deleteBucket(outputBucketName)
    }
  }
}
