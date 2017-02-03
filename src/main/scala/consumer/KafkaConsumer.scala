package consumer

import java.util.Properties

import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

import scala.concurrent.{ExecutionContext, Future}

object KafkaConsumer  {

  def consumerConnector(props: Properties): ConsumerConnector = {
    val consumerConfig = new ConsumerConfig(props)

    println("consumer.timeout.ms = " + consumerConfig.consumerTimeoutMs)
    Consumer.create(consumerConfig)
  }

  def kafkaStreams[K, V](topicsAndThreads: Map[String, Int],
                         connector: ConsumerConnector,
                         keyDecoder: Decoder[K],
                         valueDecoder: Decoder[V]): Map[String, List[KafkaStream[K, V]]] = {
    connector.createMessageStreams(topicsAndThreads, keyDecoder, valueDecoder).toMap
  }


  /**
   *
   * Warning: If you don't set the consumer.timeout.ms property in the ConsumerConfig used to create your
   * ConsumerConnector, this code will block until there are workChunkSize records to be retrieved.
   * It's probably not a good idea to have totalMillisToWaitWhenPullsAreSucceeding < consumer.timeout.ms.
   */
  def nextWorkBatch[K,V](it: ConsumerIterator[K, V], workChunkSize: Int, totalMillisToWaitWhenPullsAreSucceeding: Long)(implicit ec: ExecutionContext): Future[(Option[Throwable], Seq[MessageAndMetadata[K,V]])] = {
    val startTime = System.currentTimeMillis()
    Future {
      def loop(accum: Seq[MessageAndMetadata[K,V]]): (Option[Throwable], Seq[MessageAndMetadata[K,V]]) = {
        try {
          if (accum.size == workChunkSize) (None, accum)
          else if (it.hasNext()) {
            val next = it.next
            val newAccum = accum :+ next
            if(System.currentTimeMillis() - startTime >= totalMillisToWaitWhenPullsAreSucceeding)
              (None, newAccum)
            else loop(newAccum)
            //println(s"got one in loop")
            //loop(accum :+ next)
          }
          else (None, accum)
        } catch {
          //Note: If you don't set the consumer.timeout.ms property in the ConsumerConfig used to create your
          //your ConsumerConnector, this code will block until there are workChunkSize records to be retrieved.
          case ex: kafka.consumer.ConsumerTimeoutException =>
            println(s"got ConsumerTimeoutException")
            (None, accum)
          case t: Throwable => (Some(t), accum)
        }
      }
      loop(Vector())
    }
  }

}
