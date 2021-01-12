package io.chrisdavenport.rediculous.concurrent


object Random {
  private val random = new scala.util.Random()

  def key: String = s"key-${random.alphanumeric.take(20).mkString}"
  def value: String = s"value-${random.alphanumeric.take(100).mkString}"
}