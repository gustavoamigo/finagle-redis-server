package com.twitter.finagle.redis.server.runner

import com.twitter.finagle.redis.server.protocol._
import com.twitter.io.Buf
import org.scalatest._

class CommandRunnerSpec extends FreeSpec with Matchers {
  def parseCmd(cmd: String): List[Buf] = cmd.split(' ').toList.map(e => Buf.Utf8(e))
  def runCmd(command: String, store: Store = Store()): (Resp, Store) =
    CommandRunner.run(CommandParser.apply(parseCmd(command)), store)

  "String commands" - {
    "SET key value" in {
      val (reply, kv) = runCmd("SET key value")
      reply shouldBe a[SimpleStringResp]
      val ok = reply.asInstanceOf[SimpleStringResp]
      new String(ok.str) should be ("OK")
      kv.keyValue("key".getBytes) should be ("value".getBytes)
    }

    "GET key" in {
      val (_, kv0) = runCmd("SET key value")
      val (reply, _) = runCmd("GET key", kv0)
      reply shouldBe a[BulkStringResp]
      val value = reply.asInstanceOf[BulkStringResp]
      new String(value.str) should be ("value")
    }

    "MGET key1 key2" in {
      val (_, kv0) = runCmd("SET key1 value1")
      val (_, kv1) = runCmd("SET key2 value2", kv0)
      val (reply, _) = runCmd("MGET key1 key2", kv1)
      reply shouldBe a[ArrayResp]
      val arrays = reply.asInstanceOf[ArrayResp]
      val values: List[String] = arrays.values.collect { case BulkStringResp(ba) => new String(ba)}
      values(0) should be ("value1")
      values(1) should be ("value2")
    }

    "APPEND key appended" in {
      val (_, kv0) = runCmd("SET key value")
      val (reply1, kv1) = runCmd("APPEND key appended", kv0)
      reply1 shouldBe a[IntegerResp]
      val integers = reply1.asInstanceOf[IntegerResp]
      integers.int should be (13)
      val (reply2, _) = runCmd("GET key", kv1)
      reply2 shouldBe a[BulkStringResp]
      val value = reply2.asInstanceOf[BulkStringResp]
      new String(value.str) should be ("valueappended")
    }

    "INCR key" in {
      val (_, kv0) = runCmd("SET key 1")
      val (reply1, kv1) = runCmd("INCR key", kv0)
      reply1 shouldBe a[IntegerResp]
      val integers = reply1.asInstanceOf[IntegerResp]
      integers.int should be (2)
    }

    "INCR newKey" in {
      val (reply1, kv1) = runCmd("INCR newKey")
      reply1 shouldBe a[IntegerResp]
      val integers = reply1.asInstanceOf[IntegerResp]
      integers.int should be (1)
    }
  }

  "List commands" - {
    "LPUSH myList 1" in {
      val (reply1, st1) = runCmd("LPUSH myList 1")
      reply1 shouldBe a[IntegerResp]
      val integerResp1 = reply1.asInstanceOf[IntegerResp]
      integerResp1.int should be (1)

      val (reply2, st2) = runCmd("LPUSH myList 1", st1)
      reply2 shouldBe a[IntegerResp]
      val integerResp2 = reply2.asInstanceOf[IntegerResp]
      integerResp2.int should be (2)
    }
  }

}