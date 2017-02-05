package tools

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.Config
import consumer.KafkaConsumer
import kafka.consumer.{ConsumerConnector, ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import tools.Window.{ProcessStatsWithCurrTime, WindowStatsWithEndTime}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

trait ConsumerExperiment[K,V]  {

  val config: Config
  val keyDecoder: Decoder[K]
  val valueDecoder: Decoder[V]

  def sendOutput(data: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime]): Unit

  println(s"config is $config")

  val numReceived = new AtomicInteger(0)

  object ConsumePartition {
    //TODO  Use a different executionContext here
    import scala.concurrent.ExecutionContext.Implicits.global

    def consumerProcessForAPartition[K,V](topic: String, identifier: Int, kafkaStream: KafkaStream[K, V]): Runnable = new Runnable {

      println(s"creating runnable for topic $topic. Identifier is $identifier")
      val consumerIterator: ConsumerIterator[K, V] = kafkaStream.iterator

      def run: Unit = {

        while(true) {
          val f = KafkaConsumer.nextWorkBatch(consumerIterator,
            config.getInt("work.chunk.size"),
            config.getLong("time.toWait.for.pulls.when.succceeding.in.millis")).map { x: (Option[Throwable], Seq[MessageAndMetadata[K, V]]) =>
            //println(s"In topic $topic identifier $identifier numReceived is ${x._2.size}")

            numReceived.getAndAdd(x._2.size)
            if(x._2.size > 0)
              mark(x._2.size)
            //println(s"nunReceived is ${numReceived.get}")
/*             x._2.foreach { mm =>
                println(s"message from topic ${mm.topic}, partition ${mm.partition} offset ${mm.offset}:\n" + mm.message())
              }*/

            if (x._1.isDefined) {
              println(s"error reading from topic ${topic}")
              x._1.get.printStackTrace()
            }
          }
          await(f)


        }
      }
    }

  }

  val system = ActorSystem("actor_system_for_consuming")
  import scala.concurrent.ExecutionContext.Implicits.global
  val window = system.actorOf(Props(
    new Window(new FiniteDuration(config.getLong("window.duration.in.millis"), TimeUnit.MILLISECONDS), sendOutput )

  ))

  def mark(num: Long): Unit = {
    window ! num
  }


  val topicsAndThreadNums: Map[String, Int] =  Support.toSimpleMap( config.getConfig("topicsAndThreadNums")).map{  case (topic, threads) => (topic, threads.toInt) }
  println(topicsAndThreadNums)

  val kafkaConsumerProperties = tools.Support.toProperties(config.getConfig("properties"))
  println("kafkaConsumerProperties: " + kafkaConsumerProperties)

  val consumerConnector: ConsumerConnector = KafkaConsumer.consumerConnector(kafkaConsumerProperties)
  val streamsMap: Map[String, List[KafkaStream[K, V]]] = KafkaConsumer.kafkaStreams[K,V](topicsAndThreadNums, consumerConnector, keyDecoder, valueDecoder)

  val runnables: Seq[Runnable] = streamsMap.toSeq.flatMap{ case( topicName, streams  )  =>
    (0 to streams.size).zip(streams).map{ case (identifier, theStream) =>
      ConsumePartition.consumerProcessForAPartition(topicName, identifier, theStream )

    }

  }


  val numThreadsToUse = topicsAndThreadNums.values.sum
  println(s"numThreadsToUse is $numThreadsToUse")
  val consumerEC = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(topicsAndThreadNums.values.sum))

  runnables.foreach{r: Runnable =>
    consumerEC.execute(r)
  }


  sys.addShutdownHook{
    println(s"numReceived is ${numReceived.get}")
  }

  Thread.sleep(config.getLong("max.runtime"))

  def await[T](futT: Future[T], timeout: FiniteDuration = new FiniteDuration(100, TimeUnit.SECONDS)) = Await.result(futT, timeout)


}
