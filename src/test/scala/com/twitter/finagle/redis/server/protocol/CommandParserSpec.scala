package com.twitter.finagle.redis.server.protocol

import com.twitter.io.Buf
import org.scalatest.FreeSpec
import org.scalatest._

class CommandParserSpec extends FreeSpec with Matchers {
  "CommandParser" - {
    "auxilary methods" - {
      "parts separated with space" in {
        val array = "SET A myValue".getBytes()
        val parts = CommandParser.extractParts(array)
        parts.size should be (3)
        new String(parts(0)) should be ("SET")
        new String(parts(1)) should be ("A")
        new String(parts(2)) should be ("myValue")
      }
      "parts separated with double quote" in {
        val array = "SET A \"my value\"".getBytes()
        val parts = CommandParser.extractParts(array)
        parts.size should be (3)
        new String(parts(0)) should be ("SET")
        new String(parts(1)) should be ("A")
        new String(parts(2)) should be ("my value")
      }
    }

    "GET key" in {
      val buf = Buf.Utf8("GET key")
      val command = CommandParser(buf)
      command shouldBe a[Get]
      val get = command.asInstanceOf[Get]
      new String(get.key) should be ("key")

    }

    "SET key value" in {
      val buf = Buf.Utf8("SET key value")
      val command = CommandParser(buf)
      command shouldBe a[Set]
      val set = command.asInstanceOf[Set]
      new String(set.key) should be ("key")
      new String(set.value) should be ("value")
      set.ex should be (None)
      set.px should be (None)
      set.nx should be (false)
      set.xx should be (false)
    }

    "SET key value XX" in {
      val buf = Buf.Utf8("SET key value XX")
      val command = CommandParser(buf)
      command shouldBe a[Set]
      val set = command.asInstanceOf[Set]
      new String(set.key) should be ("key")
      new String(set.value) should be ("value")
      set.ex should be (None)
      set.px should be (None)
      set.nx should be (false)
      set.xx should be (true)
    }

    "SET key value PX 12 NX" in {
      val buf = Buf.Utf8("SET key value PX 12 NX")
      val command = CommandParser(buf)
      command shouldBe a[Set]
      val set = command.asInstanceOf[Set]
      set should be (Set("key".getBytes, "value".getBytes, None, Some(12), true, false))
    }
  }


}
