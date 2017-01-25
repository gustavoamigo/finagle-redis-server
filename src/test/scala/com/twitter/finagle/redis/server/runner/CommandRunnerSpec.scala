package com.twitter.finagle.redis.server.runner

import com.twitter.finagle.redis.server.protocol._
import com.twitter.finagle.redis.server.runner.CommandRunner.KV
import com.twitter.io.Buf
import org.scalatest._

class CommandRunnerSpec extends FreeSpec with Matchers {
  def parseCmd(cmd: String): List[Buf] = cmd.split(' ').toList.map(e => Buf.Utf8(e))
  def runCmd(command: String, kv: CommandRunner.KV): (Resp, KV) =
    CommandRunner.run(CommandParser.apply(parseCmd(command)), kv)

  "SET key value" in {
    val (reply, kv) = runCmd("SET key value", Map.empty)
    reply shouldBe a[SimpleStringResp]
    val ok = reply.asInstanceOf[SimpleStringResp]
    new String(ok.str) should be ("OK")
    kv("key".getBytes) should be ("value".getBytes)
  }

  "GET key" in {
    val (_, kv0) = runCmd("SET key value", Map.empty)
    val (reply, _) = runCmd("GET key", kv0)
    reply shouldBe a[BulkStringResp]
    val value = reply.asInstanceOf[BulkStringResp]
    new String(value.str) should be ("value")
  }

  "MGET key1 key2" in {
    val (_, kv0) = runCmd("SET key1 value1", Map.empty)
    val (_, kv1) = runCmd("SET key2 value2", kv0)
    val (reply, _) = runCmd("MGET key1 key2", kv1)
    reply shouldBe a[ArrayResp]
    val arrays = reply.asInstanceOf[ArrayResp]
    val values: List[String] = arrays.values.collect { case BulkStringResp(ba) => new String(ba)}
    values(0) should be ("value1")
    values(1) should be ("value2")
  }

  "APPEND key appended" in {
    val (_, kv0) = runCmd("SET key value", Map.empty)
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
    val (_, kv0) = runCmd("SET key 1", Map.empty)
    val (reply1, kv1) = runCmd("INCR key", kv0)
    reply1 shouldBe a[IntegerResp]
    val integers = reply1.asInstanceOf[IntegerResp]
    integers.int should be (2)
  }

  "INCR newKey" in {
    val (reply1, kv1) = runCmd("INCR newKey", Map.empty)
    reply1 shouldBe a[IntegerResp]
    val integers = reply1.asInstanceOf[IntegerResp]
    integers.int should be (1)
  }
}