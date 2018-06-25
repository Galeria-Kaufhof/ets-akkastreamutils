package de.kaufhof.ets.akkastreamutils.charset

import akka.Done
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import de.kaufhof.ets.akkastreamutils.core.StreamUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class StreamUtilsTest extends WordSpec with Matchers with BeforeAndAfterAll {
  import CharDecoderTest._

  implicit val system: ActorSystem = ActorSystem("StreamUtilsTest")

  override def afterAll(): Unit = {
    super.afterAll()
  }

  case object TestException extends Exception("TestExeption")

  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  def await[T](f: => Future[T], timeout: FiniteDuration = 5.seconds): T = Await.result(f, timeout)
  def awaitCatch[T](f: => Future[T], timeout: FiniteDuration = 5.seconds): Try[T] = Try(Await.result(f, timeout))

  def testFlow(testBytes: Array[Byte], flow: Flow[ByteString, String, _]): String = {
    await(
      Source(testBytes.grouped(1).toList.map(ByteString(_)))
        .via(flow)
        .toMat(Sink.fold("")(_ + _))(Keep.right)
        .run()
    )
  }

  "StreamUtils" should {

    "decode with given charset" in {
      testFlow(utf8Bytes, StreamUtils.decodeCharFlow(utf8)) shouldEqual testString
      testFlow(windowsBytes, StreamUtils.decodeCharFlow(windows1252)) shouldEqual testString
    }

    "connect sink to source " in {
      val testSource = Source(1 to 10)
      val nextSource = testSource.runWith(StreamUtils.fixedBroadcastHub)

      await(nextSource.runWith(Sink.seq)) shouldEqual (1 to 10)
    }

    "propagate failure from sink to source " in {
      val testSource = Source.failed[Int](TestException)
      val nextSource = testSource.runWith(StreamUtils.fixedBroadcastHub)

      assertThrows[TestException.type] {
        await(nextSource.runWith(Sink.seq))
      }
    }

    "propagate later failure from sink to source " in {
      val testSource = Source(1 to 10).map(i => if (i == 8) throw TestException else i)
      val nextSource = testSource.runWith(StreamUtils.fixedBroadcastHub)

      assertThrows[TestException.type] {
        await(nextSource.runWith(Sink.seq))
      }
    }

    "strip bom" in {
      val bom = List[Byte](-17, -69, -65)
      val testStr = "Test".getBytes("UTF-8").toList
      val result = await(Source((bom ++ testStr).map(ByteString(_))).via(StreamUtils.stripBomFlow).runWith(Sink.fold(ByteString.empty)(_ ++ _)))

      result.utf8String shouldEqual "Test"

      val result2 = await(Source.single(ByteString("A")).via(StreamUtils.stripBomFlow).runWith(Sink.fold(ByteString.empty)(_ ++ _)))

      result2.utf8String shouldEqual "A"
    }

    "fail on io error" in {
      val srcOk = Source.single(1).mapMaterializedValue(_ => Future.successful(IOResult(1L, Success(Done))))
      val srcFail = Source.single(1).mapMaterializedValue(_ => Future.successful(IOResult(1L, Failure(TestException))))

      awaitCatch(StreamUtils.failingIoSource(srcOk).runWith(Sink.seq)) shouldEqual Success(Seq(1))
      awaitCatch(StreamUtils.failingIoSource(srcFail).runWith(Sink.seq)) shouldEqual Failure(TestException)
    }
  }

}
