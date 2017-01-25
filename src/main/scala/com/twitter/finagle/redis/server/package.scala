package com.twitter.finagle.redis

package object server {
  implicit def nativeToByteArray(arr: Array[Byte]): ByteArrayKey = new ByteArrayKey(arr)
  implicit def byteArrayToNative(byteArray: ByteArrayKey): Array[Byte] = byteArray.byteArray
}
