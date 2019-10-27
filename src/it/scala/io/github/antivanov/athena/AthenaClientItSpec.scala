package io.github.antivanov.athena

import io.github.antivanov.athena.query.RowReader._
import org.scalatest.{FreeSpec, Matchers}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, ListBucketsRequest, ListObjectsRequest, PutObjectRequest, S3Object}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.control.NonFatal

class AthenaClientItSpec extends FreeSpec with Matchers {

  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

  val athenaDatabase = "athenaittests"
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

      //TODO: Assert the results of executing the query to get the 5 most populous cities
      try {
        val athenaClient = new AthenaClient(AthenaConfiguration(athenaDatabase, f"s3://$outputBucketName", Region.EU_CENTRAL_1))

        val createDatabaseStatement =
          f"""
             |create external table if not exists $athenaDatabase.cities(
             |  city string,
             |  population int
             |) row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
             |with serdeproperties (
             |"input.regex" = "^(\\S+),(\\S+)$$"
             |) location "s3://$outputBucketName/";
             |""".stripMargin

        //TODO: Add a more convenient API to execute statements in Athena such as database creation
        Await.result(athenaClient.executeQuery(f"create database $athenaDatabase;"), 5 seconds)
        Await.result(athenaClient.executeQuery(createDatabaseStatement), 5 seconds)
      } catch {
        case NonFatal(e) =>
          e.printStackTrace()
      }

      deleteBucket(outputBucketName)
    }
  }
}
