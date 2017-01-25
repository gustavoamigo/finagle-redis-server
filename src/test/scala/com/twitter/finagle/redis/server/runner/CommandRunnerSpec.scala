package com.twitter.finagle.redis.server.runner

import com.twitter.finagle.redis.server.protocol._
import com.twitter.finagle.redis.server.runner.CommandRunner.KV
import com.twitter.io.Buf
import org.scalatest._

class CommandRunnerSpec extends FreeSpec with Matchers {
  def runCmd(cmd: String, kv: CommandRunner.KV): (Reply, KV) =
    CommandRunner.run(CommandParser.apply(Buf.Utf8(cmd)), kv)

  "SET key value" in {
    val (reply, kv) = runCmd("SET key value", Map.empty)
    reply shouldBe a[SimpleString]
    val ok = reply.asInstanceOf[SimpleString]
    new String(ok.str) should be ("OK")
    kv("key".getBytes) should be (StringValue("value".getBytes))
  }

  "GET key" in {
    val (_, kv0) = runCmd("SET key value", Map.empty)
    val (reply, _) = runCmd("GET key", kv0)
    reply shouldBe a[BulkStrings]
    val value = reply.asInstanceOf[BulkStrings]
    new String(value.str) should be ("value")
  }

  "MGET key1 key2" in {
    val (_, kv0) = runCmd("SET key1 value1", Map.empty)
    val (_, kv1) = runCmd("SET key2 value2", kv0)
    val (reply, _) = runCmd("MGET key1 key2", kv1)
    reply shouldBe a[Arrays]
    val arrays = reply.asInstanceOf[Arrays]
    val values: List[String] = arrays.values.collect { case BulkStrings(ba) => new String(ba)}
    values(0) should be ("value1")
    values(1) should be ("value2")
  }

  "APPEND key appended" in {
    val (_, kv0) = runCmd("SET key value", Map.empty)
    val (reply1, kv1) = runCmd("APPEND key appended", kv0)
    reply1 shouldBe a[Integers]
    val integers = reply1.asInstanceOf[Integers]
    integers.int should be (13)
    val (reply2, _) = runCmd("GET key", kv1)
    reply2 shouldBe a[BulkStrings]
    val value = reply2.asInstanceOf[BulkStrings]
    new String(value.str) should be ("valueappended")

  }

}