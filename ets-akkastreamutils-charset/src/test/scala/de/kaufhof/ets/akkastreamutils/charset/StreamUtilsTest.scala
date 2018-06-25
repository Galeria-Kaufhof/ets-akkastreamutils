package de.kaufhof.ets.akkastreamutils.charset

import java.nio.charset.Charset

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

class StreamUtilsTest extends WordSpec with Matchers with BeforeAndAfterAll {
  val testString = "The qµ!©k brown fox jumps ov€r the lazy dog."
  val testStringWithInvalidChars = "The q�!�k brown fox jumps ov�r the lazy dog."

  val utf8: Charset = Charset.forName("UTF-8")
  val windows1252: Charset = Charset.forName("windows-1252")

  val utf8Bytes: Array[Byte] = testString.getBytes(utf8)
  val windowsBytes: Array[Byte] = testString.getBytes(windows1252)

  implicit val system: ActorSystem = ActorSystem("StreamUtilsTest")

  override def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
    ()
  }

  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  def await[T](f: => Future[T], timeout: FiniteDuration = 5.seconds): T = Await.result(f, timeout)

  def testFlow(testBytes: Array[Byte], flow: Flow[ByteString, String, _]): String = {
    await(
      Source(testBytes.grouped(1).toList.map(ByteString(_)))
        .via(flow)
        .toMat(Sink.fold("")(_ + _))(Keep.right)
        .run()
    )
  }

  "StreamUtils" should {

    "decode with unknown charset" in {
      testFlow(utf8Bytes, StreamUtils.detectAndDecodeCharsetFlow()) shouldEqual testString
      testFlow(windowsBytes, StreamUtils.detectAndDecodeCharsetFlow()) shouldEqual testString
    }

    "decode with unknown charset and small detection buffer" in {
      testFlow(utf8Bytes, StreamUtils.detectAndDecodeCharsetFlow(10)) shouldEqual testString
      testFlow(windowsBytes, StreamUtils.detectAndDecodeCharsetFlow(10)) shouldEqual testString
    }

  }

}
