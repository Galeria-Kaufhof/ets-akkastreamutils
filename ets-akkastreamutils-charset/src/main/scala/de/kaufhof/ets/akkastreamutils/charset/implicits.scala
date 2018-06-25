package de.kaufhof.ets.akkastreamutils.charset

import java.nio.charset.CodingErrorAction

import akka.util.ByteString
import akka.stream.scaladsl._

//Documentation see StreamUtils.<method>Flow
object implicits {

  /************* Source/Flow shared ops ****************/

  implicit class ByteStringSourceSharedOps[Mat](val source: Source[ByteString, Mat]) extends AnyVal {

    def detectAndDecodeCharset(maxDetectSize: Int = 64*1024,
                               decodeBufferSize: Int = 8192,
                               onMalformedInput: CodingErrorAction = CodingErrorAction.REPLACE,
                               defaultCharset: String = "UTF-8"): Source[String, Mat] =
      source.via(StreamUtils.detectAndDecodeCharsetFlow(maxDetectSize, decodeBufferSize, onMalformedInput, defaultCharset))
  }

  implicit class ByteStringFlowSharedOps[In, Mat](val flow: Flow[In, ByteString, Mat]) extends AnyVal {

    def detectAndDecodeCharset(maxDetectSize: Int = 64*1024,
                               decodeBufferSize: Int = 8192,
                               onMalformedInput: CodingErrorAction = CodingErrorAction.REPLACE,
                               defaultCharset: String = "UTF-8"): Flow[In, String, Mat] =
      flow.via(StreamUtils.detectAndDecodeCharsetFlow(maxDetectSize, decodeBufferSize, onMalformedInput, defaultCharset))
  }

}
