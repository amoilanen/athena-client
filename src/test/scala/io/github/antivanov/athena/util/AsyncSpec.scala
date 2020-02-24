package io.github.antivanov.athena.util

import java.util.concurrent.{Executors, ScheduledExecutorService}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}
import org.scalatest.time._
import Async.{checkAtIntervalUntilReady, checkOnce}
import org.scalamock.scalatest.MockFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class AsyncSpec extends FreeSpec with Matchers with ScalaFutures with MockFactory {

  implicit val defaultPatience =
    PatienceConfig(timeout =  Span(1, Seconds), interval = Span(10, Millis))
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  "checkOnce" - {

    implicit val scheduledExecutorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

    val value = 4

    "block successfully finishes delay - should return a succeeding Future" in {
      val block = () => Future.successful(value)
      checkOnce(block).futureValue shouldEqual value
    }

    "block fails delay - should return a failing Future" in {
      val exception = new RuntimeException("Check failed")
      val block = () => Future.failed[Int](exception)

      Try(checkOnce(block).futureValue) shouldBe Symbol("failure")
    }
  }

  "checkAtIntervalUntilReady" - {

    val value = "abc"
    val waitingIntervalMs = 50
    val checkNumberBeforeTimeout = 3
    val timeoutMs = (checkNumberBeforeTimeout - 1) * waitingIntervalMs

    trait Checkable[T] {

      def check(): Option[T]
    }

    "block returns immediately - should return a succeeding Future" in {
      def checkValue(): Option[String] = Option(value)

      checkAtIntervalUntilReady[String](checkValue)(waitingIntervalMs, timeoutMs).futureValue shouldEqual value
    }

    "block returns before timeout expires - should return a succeeding Future" in {
      val checkable = stub[Checkable[String]]
      (checkable.check _).when().returns(None).noMoreThanOnce()
      (checkable.check _).when().returns(Option(value)).noMoreThanOnce()

      checkAtIntervalUntilReady[String](checkable.check _)(waitingIntervalMs, timeoutMs).futureValue shouldEqual value
      (checkable.check _).verify().twice()
    }

    "block does not return before timeout expires - should return a failing Future" in {
      val checkable = stub[Checkable[String]]
      (1 to checkNumberBeforeTimeout).foreach { _ =>
        (checkable.check _).when().returns(None).noMoreThanOnce()
      }
      (checkable.check _).when().returns(Option(value)).noMoreThanOnce()

      assertThrows[RuntimeException] {
        checkAtIntervalUntilReady[String](checkable.check _)(waitingIntervalMs, timeoutMs).futureValue
      }
      (checkable.check _).verify().repeat(checkNumberBeforeTimeout)
    }

    "block never returns - should return a failing Future" in {
      val checkable = stub[Checkable[String]]
      (checkable.check _).when().returns(None)

      assertThrows[RuntimeException] {
        checkAtIntervalUntilReady[String](checkable.check _)(waitingIntervalMs, timeoutMs).futureValue
      }
      (checkable.check _).verify().repeat(checkNumberBeforeTimeout)
    }
  }
}
