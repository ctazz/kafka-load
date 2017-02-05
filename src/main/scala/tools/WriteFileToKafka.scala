package tools

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer._
import tools.Window.{Report, WindowStatsWithEndTime, ProcessStatsWithCurrTime}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait WriteFileToKafka  {

  import Support._

  val config: Config
  def sendOutput(data: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime]): Unit

  val producer = kafkaProducer[String, String](toProperties(config.getConfig("properties")))

  val topicName: String = config.getString("topicName")
  val numRecordsToWrite = config.getInt("numRecordsToWrite") //2000
  val numWriters = config.getInt("numWriters")
  val restBetweenWrites = config.getInt("restBetweenWrites")

  val messagesAsText = """
[    {        "ingest": {            "time": "2017-01-09T22:36:26.8210000Z",            "clientIp": "13.66.226.137",            "quality": 24736        },        "name": "Microsoft.ApplicationInsights.Dev.aif5b364b0890bb4793940f4e05f5ccdb75.Event",        "time": "2017-01-09T22:36:18.2050438Z",        "iKey": "AIF-5b364b08-90bb-4793-940f-4e05f5ccdb75",        "tags": {            "ai.internal.sdkVersion": "dotnet: 2.1.0.26048"        },        "data": {            "baseType": "EventData",            "baseData": {                "ver": 2,                "name": "LoadPlan",                "properties": {                    "MsitPartB": "true",                    "DeveloperMode": "true",                    "deliveryOrderId": "8014261842",                    "AppAction": "LoadPlan.Received.[081466849].[RBTW]",                    "countryCode": "USA",                    "applicationName": "DTVIntegration",                    "EventOccurrenceTime": "2017-01-09T22:36:18.3631594Z",                    "EventType": "BusinessProcessEvent",                    "UserRoleName": "MININT-EGTRAM3.fareast.corp.microsoft.com",                    "processName": "Process204",                    "XCV": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "CorrelationId": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "PartnerId": "RBTW",                    "btsMessageType": "http://schemas.microsoft.com/BizTalk/EDI/X12/2006#X12_00401_204",                    "environment": "UAT",                    "controlNumber": "1388726",                    "stageName": "ReceivedEDI204Message",                    "TemplateType": "Internal.BusinessProcessEvent",                    "taskId": "204",                    "transactionId": "6E0FE8A7-EF0B-40F5-AF36-2BD6742B0BAC",                    "plantCode": "4260",                    "eventSource": "MS.SupplyChain.SupplyChainEvent",                    "BusinessProcessName": "LoadPlan",                    "parentDONumber": "212942814",                    "ComponentType": "BackgroundProcess",                    "TestBenchProcess": "1",                    "SenderID": "081466849",                    "hub": "DEV Extranet",                    "UniqueTestKey": "P139-T15312",                    "creationDateTime": "2016-11-03 18:29:10.483",                    "entityId": "212942814",                    "vendorNumber": "RBTW",                    "loadPlanId": "9807A228-2C85-4775-9A0E-2CE06758BE90"                }            }        },        "EventProcessedUtcTime": "2017-01-09T22:36:27.3453679Z",        "PartitionId": 0,        "EventEnqueuedUtcTime": "2017-01-09T22:36:27.0970000Z"    },
    {        "ingest": {            "time": "2017-01-09T22:36:26.8210000Z",            "clientIp": "13.66.226.137",            "quality": 24736        },        "name": "Microsoft.ApplicationInsights.Dev.aif5b364b0890bb4793940f4e05f5ccdb75.Event",        "time": "2017-01-09T22:36:23.2423824Z",        "iKey": "AIF-5b364b08-90bb-4793-940f-4e05f5ccdb75",        "tags": {            "ai.internal.sdkVersion": "dotnet: 2.1.0.26048"        },        "data": {            "baseType": "EventData",            "baseData": {                "ver": 2,                "name": "LoadPlan",                "properties": {                    "MsitPartB": "true",                    "DeveloperMode": "true",                    "deliveryOrderId": "8014261842",                    "AppAction": "LoadPlan.FuncAck.Send997ToPartner.[081466849].[RBTW]",                    "countryCode": "USA",                    "applicationName": "DTVIntegration",                    "EventOccurrenceTime": "2017-01-09T22:36:23.4023703Z",                    "EventType": "BusinessProcessEvent",                    "UserRoleName": "MININT-EGTRAM3.fareast.corp.microsoft.com",                    "processName": "Process204",                    "XCV": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "CorrelationId": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "PartnerId": "RBTW",                    "btsMessageType": "http://schemas.microsoft.com/BizTalk/EDI/X12/2006#X12_00401_204",                    "environment": "UAT",                    "controlNumber": "1388726",                    "stageName": "ReceivedEDI204Message",                    "TemplateType": "Internal.BusinessProcessEvent",                    "taskId": "204",                    "transactionId": "6E0FE8A7-EF0B-40F5-AF36-2BD6742B0BAC",                    "sequenceNumber": "1",                    "plantCode": "4260",                    "eventSource": "MS.SupplyChain.SupplyChainEvent",                    "BusinessProcessName": "LoadPlan",                    "parentDONumber": "212942814",                    "ComponentType": "BackgroundProcess",                    "TestBenchProcess": "1",                    "SenderID": "081466849",                    "hub": "DEV Extranet",                    "UniqueTestKey": "P139-T15312",                    "creationDateTime": "2016-11-03 18:29:10.483",                    "etwTraceId": "11e02376-00ce-420e-9e49-fbde698e2490",                    "entityId": "212942814",                    "vendorNumber": "RBTW",                    "vortextransmissionuri": "https://vortex.data.microsoft.com/collect/v1"                }            }        },        "EventProcessedUtcTime": "2017-01-09T22:36:27.3453679Z",        "PartitionId": 0,        "EventEnqueuedUtcTime": "2017-01-09T22:36:27.0970000Z"    },
    {        "ingest": {            "time": "2017-01-09T22:36:57.0240000Z",            "clientIp": "13.66.226.137",            "quality": 24736        },        "name": "Microsoft.ApplicationInsights.Dev.aif5b364b0890bb4793940f4e05f5ccdb75.Event",        "time": "2017-01-09T22:36:28.2636814Z",        "iKey": "AIF-5b364b08-90bb-4793-940f-4e05f5ccdb75",        "tags": {            "ai.internal.sdkVersion": "dotnet: 2.1.0.26048"        },        "data": {            "baseType": "EventData",            "baseData": {                "ver": 2,                "name": "LoadPlan",                "properties": {                    "MsitPartB": "true",                    "DeveloperMode": "true",                    "deliveryOrderId": "8014261842",                    "AppAction": "LoadPlan.FuncAck.PosMDNFromPartner.[081466849].[RBTW]",                    "countryCode": "USA",                    "applicationName": "DTVIntegration",                    "EventOccurrenceTime": "2017-01-09T22:36:28.4230680Z",                    "EventType": "BusinessProcessEvent",                    "UserRoleName": "MININT-EGTRAM3.fareast.corp.microsoft.com",                    "processName": "Process204",                    "XCV": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "CorrelationId": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "PartnerId": "RBTW",                    "btsMessageType": "http://schemas.microsoft.com/BizTalk/EDI/X12/2006#X12_00401_204",                    "environment": "UAT",                    "controlNumber": "1388726",                    "stageName": "ReceivedEDI204Message",                    "TemplateType": "Internal.BusinessProcessEvent",                    "taskId": "204",                    "transactionId": "6E0FE8A7-EF0B-40F5-AF36-2BD6742B0BAC",                    "sequenceNumber": "1",                    "plantCode": "4260",                    "eventSource": "MS.SupplyChain.SupplyChainEvent",                    "BusinessProcessName": "LoadPlan",                    "parentDONumber": "212942814",                    "ComponentType": "BackgroundProcess",                    "TestBenchProcess": "1",                    "SenderID": "081466849",                    "hub": "DEV Extranet",                    "UniqueTestKey": "P139-T15312",                    "creationDateTime": "2016-11-03 18:29:10.483",                    "etwTraceId": "11e02376-00ce-420e-9e49-fbde698e2490",                    "entityId": "212942814",                    "vendorNumber": "RBTW",                    "vortextransmissionuri": "https://vortex.data.microsoft.com/collect/v1"                }            }        },        "EventProcessedUtcTime": "2017-01-09T22:36:57.3819566Z",        "PartitionId": 2,        "EventEnqueuedUtcTime": "2017-01-09T22:36:56.8660000Z"    },
    {        "ingest": {            "time": "2017-01-09T22:36:57.0240000Z",            "clientIp": "13.66.226.137",            "quality": 24736        },        "name": "Microsoft.ApplicationInsights.Dev.aif5b364b0890bb4793940f4e05f5ccdb75.Event",        "time": "2017-01-09T22:36:33.3028837Z",        "iKey": "AIF-5b364b08-90bb-4793-940f-4e05f5ccdb75",        "tags": {            "ai.internal.sdkVersion": "dotnet: 2.1.0.26048"        },        "data": {            "baseType": "EventData",            "baseData": {                "ver": 2,                "name": "LoadPlan",                "properties": {                    "MsitPartB": "true",                    "DeveloperMode": "true",                    "deliveryOrderId": "8014261842",                    "AppAction": "LoadPlan.SentToPartner.[081466849].[CMCMSFT]",                    "countryCode": "USA",                    "applicationName": "DTVIntegration",                    "EventOccurrenceTime": "2017-01-09T22:36:33.4633823Z",                    "EventType": "BusinessProcessEvent",                    "UserRoleName": "MININT-EGTRAM3.fareast.corp.microsoft.com",                    "processName": "Process204",                    "XCV": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "CorrelationId": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "PartnerId": "RBTW",                    "btsMessageType": "http://schemas.microsoft.com/BizTalk/EDI/X12/2006#X12_00401_204",                    "environment": "UAT",                    "controlNumber": "1388726",                    "stageName": "ReceivedEDI204Message",                    "TemplateType": "Internal.BusinessProcessEvent",                    "taskId": "204",                    "transactionId": "6E0FE8A7-EF0B-40F5-AF36-2BD6742B0BAC",                    "sequenceNumber": "1",                    "plantCode": "4260",                    "eventSource": "MS.SupplyChain.SupplyChainEvent",                    "BusinessProcessName": "LoadPlan",                    "parentDONumber": "212942814",                    "ComponentType": "BackgroundProcess",                    "TestBenchProcess": "1",                    "SenderID": "081466849",                    "hub": "DEV Extranet",                    "UniqueTestKey": "P139-T15312",                    "creationDateTime": "2016-11-03 18:29:10.483",                    "etwTraceId": "11e02376-00ce-420e-9e49-fbde698e2490",                    "entityId": "212942814",                    "vendorNumber": "RBTW",                    "vortextransmissionuri": "https://vortex.data.microsoft.com/collect/v1",                    "loadPlanId": "9807A228-2C85-4775-9A0E-2CE06758BE90"                }            }        },        "EventProcessedUtcTime": "2017-01-09T22:36:57.3819566Z",        "PartitionId": 2,        "EventEnqueuedUtcTime": "2017-01-09T22:36:56.8660000Z"    },
    {        "ingest": {            "time": "2017-01-09T22:36:57.0240000Z",            "clientIp": "13.66.226.137",            "quality": 24736        },        "name": "Microsoft.ApplicationInsights.Dev.aif5b364b0890bb4793940f4e05f5ccdb75.Event",        "time": "2017-01-09T22:36:38.3474414Z",        "iKey": "AIF-5b364b08-90bb-4793-940f-4e05f5ccdb75",        "tags": {            "ai.internal.sdkVersion": "dotnet: 2.1.0.26048"        },        "data": {            "baseType": "EventData",            "baseData": {                "ver": 2,                "name": "LoadPlan",                "properties": {                    "MsitPartB": "true",                    "DeveloperMode": "true",                    "AppAction": "LoadPlan.TechAck.PosMDNFromPartner.[081466849].[CMCMSFT]",                    "countryCode": "USA",                    "applicationName": "DTVIntegration",                    "EventOccurrenceTime": "2017-01-09T22:36:38.5066948Z",                    "EventType": "BusinessProcessEvent",                    "UserRoleName": "MININT-EGTRAM3.fareast.corp.microsoft.com",                    "processName": "Process204",                    "XCV": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "CorrelationId": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "PartnerId": "RBTW",                    "btsMessageType": "http://schemas.microsoft.com/BizTalk/EDI/X12/2006#X12_00401_204",                    "environment": "UAT",                    "controlNumber": "1388726",                    "stageName": "ReceivedEDI204Message",                    "TemplateType": "Internal.BusinessProcessEvent",                    "taskId": "204",                    "transactionId": "6E0FE8A7-EF0B-40F5-AF36-2BD6742B0BAC",                    "sequenceNumber": "1",                    "plantCode": "4260",                    "eventSource": "MS.SupplyChain.SupplyChainEvent",                    "BusinessProcessName": "LoadPlan",                    "ComponentType": "BackgroundProcess",                    "TestBenchProcess": "1",                    "SenderID": "081466849",                    "hub": "DEV Extranet",                    "UniqueTestKey": "P139-T15312",                    "creationDateTime": "2016-11-03 18:29:10.483",                    "etwTraceId": "11e02376-00ce-420e-9e49-fbde698e2490",                    "entityId": "212942814",                    "vendorNumber": "RBTW",                    "vortextransmissionuri": "https://vortex.data.microsoft.com/collect/v1",                    "loadPlanId": "9807A228-2C85-4775-9A0E-2CE06758BE90"                }            }        },        "EventProcessedUtcTime": "2017-01-09T22:36:57.3819566Z",        "PartitionId": 2,        "EventEnqueuedUtcTime": "2017-01-09T22:36:56.8660000Z"    },
    {        "ingest": {            "time": "2017-01-09T22:36:57.0240000Z",            "clientIp": "13.66.226.137",            "quality": 24736        },        "name": "Microsoft.ApplicationInsights.Dev.aif5b364b0890bb4793940f4e05f5ccdb75.Event",        "time": "2017-01-09T22:36:43.4897196Z",        "iKey": "AIF-5b364b08-90bb-4793-940f-4e05f5ccdb75",        "tags": {            "ai.internal.sdkVersion": "dotnet: 2.1.0.26048"        },        "data": {            "baseType": "EventData",            "baseData": {                "ver": 2,                "name": "LoadPlan",                "properties": {                    "MsitPartB": "true",                    "DeveloperMode": "true",                    "AppAction": "LoadPlan.FuncAck.Pos997FromPartner.[081466849].[CMCMSFT]",                    "applicationName": "DTVIntegration",                    "EventOccurrenceTime": "2017-01-09T22:36:43.6503266Z",                    "EventType": "BusinessProcessEvent",                    "UserRoleName": "MININT-EGTRAM3.fareast.corp.microsoft.com",                    "processName": "Process204",                    "XCV": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "CorrelationId": "2b14133e-76d1-41f6-bed6-2e89c8345532",                    "PartnerId": "RBTW",                    "btsMessageType": "http://schemas.microsoft.com/BizTalk/EDI/X12/2006#X12_00401_204",                    "environment": "UAT",                    "controlNumber": "1388726",                    "stageName": "ReceivedEDI204Message",                    "TemplateType": "Internal.BusinessProcessEvent",                    "taskId": "204",                    "transactionId": "6E0FE8A7-EF0B-40F5-AF36-2BD6742B0BAC",                    "sequenceNumber": "1",                    "plantCode": "4260",                    "eventSource": "MS.SupplyChain.SupplyChainEvent",                    "BusinessProcessName": "LoadPlan",                    "ComponentType": "BackgroundProcess",                    "TestBenchProcess": "1",                    "SenderID": "081466849",                    "hub": "DEV Extranet",                    "UniqueTestKey": "P139-T15312",                    "creationDateTime": "2016-11-03 18:29:10.483",                    "etwTraceId": "11e02376-00ce-420e-9e49-fbde698e2490",                    "entityId": "212942814",                    "vendorNumber": "RBTW",                    "vortextransmissionuri": "https://vortex.data.microsoft.com/collect/v1"                }            }        },        "EventProcessedUtcTime": "2017-01-09T22:36:57.3819566Z",        "PartitionId": 2,        "EventEnqueuedUtcTime": "2017-01-09T22:36:56.8660000Z"    }]
                       """
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
      println(s"inside run")
      while(count.get() <= numRecordsToWrite) {
        producer.send(new ProducerRecord(topicName, messagesAsText), new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception != null)
              println(s"error writing data $messagesAsText")
            else {
              numWritten.getAndIncrement()
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
  }

  Thread.sleep(config.getLong("max.runtime"))


}
