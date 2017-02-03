package tools

import java.util.Properties

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.KafkaProducer

import scala.io.Source

object Support extends App {

  import scala.collection.JavaConverters._

  def readText(fileName: String): String = {
    val source = Source.fromFile(fileName)
    try source.getLines().mkString("\n") finally source.close
  }


  def createObjectMapper(): ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.registerModule(new JodaModule())
    m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    m
  }

  /**
   *
   * @param config An instance of Typesafe Config
   * @param excludeThese Set of keys you don't want in the Map
   * @return A Map[String, String].  Warning: All unwrapped values are converted to Strings, so
   *         you might not get what you expect with something such as: theHosts = [127.0.0.1:80, 10.0.0.37:80]
   */
  def toSimpleMap(config: Config, excludeThese: Set[String] = Set.empty): Map[String, String] = {
    config.entrySet.asScala.foldLeft(Map.empty[String, String]) { (acc, configEntry) =>
      if(excludeThese.contains(configEntry.getKey)) acc
      else acc + (configEntry.getKey -> configEntry.getValue.unwrapped.toString)
    }
  }


  /**
   *
   * @param props A Properties object
   * @return A Map[String, String].
   *         Warning: If you have a key or a value that isn't a String, you'll get a ClassCastException
   *         Properties objects are supposed to have keys and values that are both Strings,
   *         That's just the nature of Properties, which have a getProperty method that accepts
   *         a String and returns a String, but extends HashTable<Object, Object>.
   */
  def toSimpleMap(props: Properties): Map[String, String] = props.asScala.toMap


  /**
   *
   * @param config An instance of Typesafe Config
   * @param excludeThese Set of keys you don't want in the Map
   * @return A Properties object that has all values converted to String.
   *         If that's not what you want, don't use this function/
   */
  def toProperties(config: Config, excludeThese: Set[String] = Set.empty): Properties = toSimpleMap(config, excludeThese).foldLeft(new Properties) { (props, pair) => props.put(pair._1, pair._2); props }


  def kafkaProducer[K,V](properties: Properties):  KafkaProducer[K,V] = new KafkaProducer(properties)
}