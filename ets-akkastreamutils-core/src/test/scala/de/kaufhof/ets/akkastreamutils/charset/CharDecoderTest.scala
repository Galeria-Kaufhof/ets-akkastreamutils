package de.kaufhof.ets.akkastreamutils.charset

import java.nio.charset.Charset

import de.kaufhof.ets.akkastreamutils.core.CharDecoder
import org.scalatest.{Matchers, WordSpec}

class CharDecoderTest extends WordSpec with Matchers {
  import CharDecoderTest._

  "CharDecoder" should {
    "decode valid, non chunked UTF-8 and windows-1252" in {
      val utf8Decoder = new CharDecoder(utf8)
      val windowsDecoder = new CharDecoder(windows1252)

      utf8Decoder.decode(utf8Bytes) shouldEqual testString
      windowsDecoder.decode(windowsBytes) shouldEqual testString
    }

    "decode valid, non chunked UTF-8 and windows-1252 with small buffer" in {
      val utf8Decoder = new CharDecoder(utf8, 4)
      val windowsDecoder = new CharDecoder(windows1252, 4)

      utf8Decoder.decode(utf8Bytes) shouldEqual testString
      windowsDecoder.decode(windowsBytes) shouldEqual testString
    }

    "decode valid, chunked UTF-8 and windows-1252" in {
      val utf8Decoder = new CharDecoder(utf8)
      val windowsDecoder = new CharDecoder(windows1252)

      utf8Bytes.grouped(1).map(utf8Decoder.decode).reduceLeft(_ + _) shouldEqual testString
      utf8Bytes.grouped(2).map(utf8Decoder.decode).reduceLeft(_ + _) shouldEqual testString

      windowsBytes.grouped(1).map(windowsDecoder.decode).reduceLeft(_ + _) shouldEqual testString
      windowsBytes.grouped(2).map(windowsDecoder.decode).reduceLeft(_ + _) shouldEqual testString
    }

    "decode invalid charset with unknown characters" in {
      val utf8Decoder = new CharDecoder(utf8)

      utf8Decoder.decode(windowsBytes) shouldEqual testStringWithInvalidChars
    }
  }
}

object CharDecoderTest{
  val testString = "The qµ!©k brown fox jumps ov€r the lazy dog."
  val testStringWithInvalidChars = "The q�!�k brown fox jumps ov�r the lazy dog."

  val utf8: Charset = Charset.forName("UTF-8")
  val windows1252: Charset = Charset.forName("windows-1252")

  val utf8Bytes: Array[Byte] = testString.getBytes(utf8)
  val windowsBytes: Array[Byte] = testString.getBytes(windows1252)
}
