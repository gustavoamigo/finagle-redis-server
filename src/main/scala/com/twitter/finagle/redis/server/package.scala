package com.twitter.finagle.redis

package object server {
  implicit def nativeToByteArray(arr: Array[Byte]): ByteArrayWrapper = new ByteArrayWrapper(arr)
  implicit def byteArrayToNative(byteArray: ByteArrayWrapper): Array[Byte] = byteArray.byteArray
}
