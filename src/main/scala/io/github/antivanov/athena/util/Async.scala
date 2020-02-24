package io.github.antivanov.athena.util

import java.util.{Timer, TimerTask}
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Async {

  def checkOnce[T](delayMs: Int = 0)(block: () => Future[T])(implicit ec: ExecutionContext, scheduler: ScheduledExecutorService): Future[T] = {
    val promise = Promise[T]
    scheduler.schedule(new Runnable {
      override def run(): Unit = {
        block().map { value =>
          promise.success(value)
        } recover {
          case error: Throwable =>
            promise.failure(error)
        }
      }
    }, delayMs, TimeUnit.MILLISECONDS)
    promise.future
  }

  def checkAtIntervalUntilReady[T](intervalMs: Int, timeoutMs: Int)(block: () => Option[T])(implicit ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]
    val timer = new Timer()
    var executionTimes = 0

    val checkTask = new TimerTask {
      def run(): Unit = {
        val checkResult = Try(block())
        executionTimes = executionTimes + 1
        if (executionTimes * intervalMs > timeoutMs) {
          promise.failure(new RuntimeException("Waiting for results timed out"))
        } else {
          checkResult match {
            case Success(result) =>
              result.map(promise.success(_))
            case Failure(exception) =>
              promise.failure(exception)
          }
        }
      }
    }
    timer.schedule(checkTask, 0L, intervalMs)
    val future = promise.future
    future.onComplete {_ =>
      checkTask.cancel()
    }
    future
  }
}
