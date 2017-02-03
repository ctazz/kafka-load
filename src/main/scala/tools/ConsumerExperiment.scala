package tools

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import consumer.KafkaConsumer
import kafka.consumer.{ConsumerConnector, ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

object ConsumerExperiment extends App {

  val numReceived = new AtomicInteger(0)

  object ConsumePartition {
    //TODO  Use a different executionContext here
    import scala.concurrent.ExecutionContext.Implicits.global

    def consumerProcessForAPartition(topic: String, identifier: Int, kafkaStream: KafkaStream[String, String]): Runnable = new Runnable {

      println(s"creating runnable for topic $topic. Identifier is $identifier")
      val consumerIterator: ConsumerIterator[String, String] = kafkaStream.iterator

      def run: Unit = {

        while(true) {
          val f = KafkaConsumer.nextWorkBatch(consumerIterator, 100, 600).map { x: (Option[Throwable], Seq[MessageAndMetadata[String, String]]) =>
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
    new Window(new FiniteDuration(2, TimeUnit.SECONDS), tup => println("Read: " + Window.defaultDump(tup)) )

  ))

  def mark(num: Long): Unit = {
    window ! num
  }

  val topicsAndThreadNums: Map[String, Int] = Map("bpm_in" -> 2)


  val config = ConfigFactory.parseString(
    """
        zookeeper.connect = "127.0.0.1:2181"
        group.id = "theGroupIdw"
        client.id = testclientXXYYYwx
        zookeeper.session.timeout.ms = 400
        zookeeper.sync.time.ms = 200
        auto.commit.enable = true
        auto.commit.interval.ms = 1000
        consumer.timeout.ms = 2000
        rebalance.backoff.ms = 10000
        zookeeper.session.timeout.ms = 10000
    """
  )

  val kafkaConsumerProperties = tools.Support.toProperties(config)
  println("kafkaConsumerProperties: " + kafkaConsumerProperties)

  val consumerConnector: ConsumerConnector = KafkaConsumer.consumerConnector(kafkaConsumerProperties)
  val streamsMap: Map[String, List[KafkaStream[String, String]]] = KafkaConsumer.kafkaStreams(topicsAndThreadNums, consumerConnector, new StringDecoder(), new StringDecoder())




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

  Thread.sleep(100000)

  def await[T](futT: Future[T], timeout: FiniteDuration = new FiniteDuration(100, TimeUnit.SECONDS)) = Await.result(futT, timeout)


}
