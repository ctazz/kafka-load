package tools

import com.typesafe.config.{Config, ConfigFactory}
import kafka.serializer.{StringDecoder, Decoder}
import org.apache.kafka.clients.producer.ProducerRecord
import tools.Window.{WindowStatsWithEndTime, ProcessStatsWithCurrTime}

import scala.collection.Iterator


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

    //val sendMessagesAsSingleRecord = initialConfig.getBoolean("producer.send.messages.as.single.record ")


    new WriteToKafka[String, String] {
      override lazy val config: Config = initialConfig.getConfig("producer")
      lazy val topicName = config.getString("topicName")

      def sendOutput(data: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime]): Unit = sendFunc(data)


      def maybeIntToJavaInteger(maybeN: Option[Int]): java.lang.Integer = {
          maybeN match {
            case Some(n) => n
            case None => null
          }
      }

      def recordIterator(maybePartitionInWhichToWrite: Option[Int]): Iterator[ProducerRecord[String, String]] = {
        println(s"creating  a new recordIterator for maybePartition $maybePartitionInWhichToWrite ")
        val sendMessagesAsSingleRecord = config.getBoolean("send.messages.as.single.record ")
        if (sendMessagesAsSingleRecord) {
          new scala.Iterator[ProducerRecord[String, String]] {
            val eventsAsOneMessage = Support.readText(config.getString("records.file"))

            override def next(): ProducerRecord[String, String] = {
              new ProducerRecord[String, String](topicName,
                maybeIntToJavaInteger(maybePartitionInWhichToWrite),
                null,
                eventsAsOneMessage)
            }

            override def hasNext: Boolean = true
          }
        }
        else {
          new scala.Iterator[ProducerRecord[String, String]] {
            var counter: Long = 0
            val eventsAsSeveralMessages = Support.readText(config.getString("records.file"))
            val arrayOfMessages = eventsAsSeveralMessages.split("\n").filterNot(_.trim == "")

            override def next(): ProducerRecord[String, String] = {
              val cnt = counter
              //println(s"counter is ${cnt} and maybePartition is $maybePartitionInWhichToWrite")
              val nextMessage = arrayOfMessages((cnt % arrayOfMessages.size).toInt)
              //println(s"nextMessage is $nextMessage")

              val returnMe = new ProducerRecord[String, String](topicName,
                maybeIntToJavaInteger(maybePartitionInWhichToWrite),
                null,
                nextMessage
              )
              counter = counter + 1
              returnMe
            }

            override def hasNext: Boolean = true
          }

        }


      }

    }
  }






}
