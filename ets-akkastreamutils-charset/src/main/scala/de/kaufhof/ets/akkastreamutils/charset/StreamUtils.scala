package de.kaufhof.ets.akkastreamutils.charset

import java.nio.charset.{Charset, CodingErrorAction}

import akka.stream.scaladsl.Flow
import akka.util.ByteString
import de.kaufhof.ets.akkastreamutils.core.{StreamUtils => CoreStreamUtils, CharDecoder}
import org.mozilla.universalchardet.UniversalDetector

object StreamUtils {

  /**
    * Tries to decode a bytestring to string by detecting the charset first
    */
  def detectAndDecodeCharsetFlow(maxDetectSize: Int = 64*1024,
                                 decodeBufferSize: Int = 8192,
                                 onMalformedInput: CodingErrorAction = CodingErrorAction.REPLACE,
                                 defaultCharset: String = "UTF-8"): Flow[ByteString, String, _] =

    CoreStreamUtils.scanFinallyFlow[ByteString, String, Either[(ByteString, UniversalDetector), CharDecoder]](
      Left((ByteString.empty, new UniversalDetector(null)))
    ) {(state, newBs) =>
      val newBytes = newBs.toArray
      state match {
        case Left((bs, detector)) =>

          detector.handleData(newBytes, 0, newBytes.length)
          val nextBs = bs ++ newBs

          val charsetOpt = if (nextBs.size < maxDetectSize) {
            Option(detector.getDetectedCharset)
          } else {
            detector.dataEnd()
            Some(Option(detector.getDetectedCharset).getOrElse(defaultCharset))
          }

          charsetOpt match {
            case Some(charset) =>
              val decoder: CharDecoder = new CharDecoder(Charset.forName(charset), decodeBufferSize, onMalformedInput)
              (Right(decoder), Some(decoder.decode(nextBs.toArray)))
            case None =>
              (Left((nextBs, detector)), None)
          }

        case Right(decoder: CharDecoder) =>
          (Right(decoder), Some(decoder.decode(newBytes)))
      }
    }{
      case Left((bs, detector)) =>
        detector.dataEnd()
        val charset = Option(detector.getDetectedCharset).getOrElse(defaultCharset)
        val decoder: CharDecoder = new CharDecoder(Charset.forName(charset), decodeBufferSize, onMalformedInput)
        Some(decoder.decode(bs.toArray))
      case Right(_) => None
    }

}
