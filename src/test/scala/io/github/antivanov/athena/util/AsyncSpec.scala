package io.github.antivanov.athena.util

import org.mockito.Mockito._;
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FreeSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.time._

import scala.concurrent.ExecutionContext.Implicits.global

import Async.{check, schedule}

class AsyncSpec extends FreeSpec with Matchers with ScalaFutures with MockitoSugar {

  implicit val defaultPatience =
    PatienceConfig(timeout =  Span(1, Seconds), interval = Span(10, Millis))

  "check" - {

    val value = "abc"
    val waitingIntervalMs = 50
    val checkNumberBeforeTimeout = 3
    val timeoutMs = (checkNumberBeforeTimeout - 1) * waitingIntervalMs

    trait Checkable[T] {

      def check(): Option[T]
    }

    "block returns immediately - should return a succeeding Future" in {
      def checkValue(): Option[String] = Option(value)

      check[String](waitingIntervalMs, timeoutMs)(checkValue).futureValue shouldEqual value
    }

    "block returns before timeout expires - should return a succeeding Future" in {
      val checkable = mock[Checkable[String]]
      when(checkable.check())
        .thenReturn(None)
        .thenReturn(Option(value))

      check[String](waitingIntervalMs, timeoutMs)(checkable.check _).futureValue shouldEqual value
      verify(checkable, times(2)).check
    }

    "block does not return before timeout expires - should return a failing Future" in {
      val checkable = mock[Checkable[String]]
      val mockedCheck = (1 to checkNumberBeforeTimeout).foldLeft(when(checkable.check())) {
        case (mockedCheck, _) => mockedCheck.thenReturn(None)
      }
      mockedCheck.thenReturn(Option(value))

      assertThrows[RuntimeException] {
        check[String](waitingIntervalMs, timeoutMs)(checkable.check _).futureValue
      }
      verify(checkable, times(checkNumberBeforeTimeout)).check
    }

    "block never returns - should return a failing Future" in {
      val checkable = mock[Checkable[String]]
      when(checkable.check()).thenReturn(None)

      assertThrows[RuntimeException] {
        check[String](waitingIntervalMs, timeoutMs)(checkable.check _).futureValue
      }
      verify(checkable, times(checkNumberBeforeTimeout)).check
    }
  }
}
