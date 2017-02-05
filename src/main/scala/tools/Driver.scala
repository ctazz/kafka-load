package tools

import com.typesafe.config.{Config, ConfigFactory}
import kafka.serializer.{StringDecoder, Decoder}

import scala.util.Try


object Driver extends App {
  println("You've invoked the driver")

  val initialConfig = ConfigFactory.load
  println(s"initialConfig is $initialConfig")

  if(initialConfig.getBoolean("read")) {
    new ConsumerExperiment[String, String] {
      override lazy val config = initialConfig.getConfig("consumer")
      override val keyDecoder: Decoder[String] = new StringDecoder()
      override val valueDecoder: Decoder[String] = new StringDecoder()
    }
  }
  else {
    new WriteFileToKafka {
      override lazy val config: Config = initialConfig.getConfig("producer")
    }
  }






}
