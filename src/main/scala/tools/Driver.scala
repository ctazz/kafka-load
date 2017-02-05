package tools

import com.typesafe.config.{Config, ConfigFactory}
import kafka.serializer.{StringDecoder, Decoder}
import tools.Window.{WindowStatsWithEndTime, ProcessStatsWithCurrTime}

import scala.util.Try


object Driver extends App {

  val initialConfig = ConfigFactory.load
  val consume = initialConfig.getBoolean("read")
  val outputPrefix = if(consume) "Read: " else "Write: "

  def sendFunc(data: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime]): Unit = {
    println(outputPrefix + Window.defaultDump(data))
  }

  if(consume) {
    new ConsumerExperiment[String, String] {
      override lazy val config = initialConfig.getConfig("consumer")
      override val keyDecoder: Decoder[String] = new StringDecoder()
      override val valueDecoder: Decoder[String] = new StringDecoder()
      def sendOutput(data: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime]): Unit = sendFunc(data)
    }
  }
  else {
    new WriteFileToKafka {
      override lazy val config: Config = initialConfig.getConfig("producer")
      def sendOutput(data: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime]): Unit = sendFunc(data)
    }
  }






}
