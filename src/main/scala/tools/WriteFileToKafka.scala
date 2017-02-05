package tools

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer._
import tools.Window.{Report, WindowStatsWithEndTime, ProcessStatsWithCurrTime}

import scala.collection.Iterator
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait WriteFileToKafka[K, V]  {

  import Support._

  val config: Config
  def sendOutput(data: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime]): Unit
  def recordIterator: Iterator[ProducerRecord[K,V]]

  val producer = kafkaProducer[K, V](toProperties(config.getConfig("properties")))

  val numRecordsToWrite = config.getInt("numRecordsToWrite")
  val numWriters = config.getInt("numWriters")
  val restBetweenWrites = config.getInt("restBetweenWrites")

  val system = ActorSystem("actor_system_for_producing")
  import scala.concurrent.ExecutionContext.Implicits.global
  val window = system.actorOf(Props(
      new Window(new FiniteDuration(config.getLong("window.duration.in.millis"), TimeUnit.MILLISECONDS), sendOutput )

  ))

  def mark(num: Long): Unit = {
    window ! num
  }


  val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numWriters))

  def runnable = new Runnable {

    val count = new AtomicInteger(1)
    val numWritten = new AtomicInteger(0)
    val startTime = System.currentTimeMillis()

    override def run(): Unit = {
      while(count.get() <= numRecordsToWrite && recordIterator.hasNext) {
        val producerRecord: ProducerRecord[K,V] = recordIterator.next
        producer.send(recordIterator.next(), new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception != null)
              println(s"error writing data ${producerRecord}")
            else {
              mark(1)
              //println(s"Kafka record written to topic/partition/offset ${metadata.topic}/${metadata.partition}/${metadata.offset}, data is $messagesAsText")
            }

          }
        })
        val currCount = count.getAndIncrement
        Thread.sleep(restBetweenWrites)
      }
          Thread.sleep(10)
          println(s"num sent is $numWritten in ${System.currentTimeMillis() - startTime}  ${Thread.currentThread().getName}")
          Thread.sleep(10)
          println(s"num sent is $numWritten in ${System.currentTimeMillis() - startTime} ${Thread.currentThread().getName}")
          Thread.sleep(100)
          println(s"num sent is $numWritten in ${System.currentTimeMillis() - startTime} ${Thread.currentThread().getName}")
          Thread.sleep(1000)
          println(s"num sent is $numWritten in ${System.currentTimeMillis() - startTime} ${Thread.currentThread().getName}")
          Thread.sleep(3000)
          println(s"num sent is $numWritten in ${System.currentTimeMillis() - startTime} ${Thread.currentThread().getName}")

    }



  }

  (1 to numWriters).foreach((_ => ec.execute(runnable)))


  sys.addShutdownHook{
    window ! Report
    producer.close()
  }

  Thread.sleep(config.getLong("max.runtime"))


}
