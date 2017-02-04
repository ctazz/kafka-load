package tools

import com.typesafe.config.ConfigFactory
import kafka.serializer.{StringDecoder, Decoder}

import scala.util.Try


object Driver extends App {
  println("You've invoked the driver")

  val initialConfig = ConfigFactory.load
  println(s"initialConfig is $initialConfig")

  Try(initialConfig.getConfig("consumer")).toOption.map{c =>
    println(s"c is $c")
    new ConsumerExperiment[String, String] {
      override lazy val config = c
      override val keyDecoder: Decoder[String] = new StringDecoder()
      override val valueDecoder: Decoder[String] = new StringDecoder()
    }

  }.getOrElse{
    println("ain't got no consumer config")
  }





}
