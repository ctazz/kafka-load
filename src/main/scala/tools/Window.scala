package tools

import akka.actor.{Actor, Cancellable}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object Window {

  trait Stats {
    def startTime: Long
    def totalReceived: Long

    def update[T <: Stats](moreReceived: Long, f:  (Long, Long) => T    ): T = {
      f( startTime, totalReceived + moreReceived   )
    }

  }
  case class ProcessStats(startTime: Long, totalReceived: Long) extends Stats {
    //TODO  Ugly repetition
    def update(moreReceived: Long): ProcessStats = update(moreReceived, ProcessStats.apply)
  }
  case class WindowStats(startTime: Long, totalReceived: Long) extends Stats {
    def update(moreReceived: Long): WindowStats = update(moreReceived, WindowStats.apply)
  }

  //Not used for messages. They are instead arguments to the sendOutput function
  case class ProcessStatsWithCurrTime(currTime: Long, processStats: ProcessStats)
  case class WindowStatsWithEndTime(end: Long, windowStats: WindowStats)

  case object Report

  def defaultDump( tup: (ProcessStatsWithCurrTime, WindowStatsWithEndTime)): String =     tup match {
    case ( ProcessStatsWithCurrTime(curr, ProcessStats(processStart, processTotal) ), WindowStatsWithEndTime(end, WindowStats(windowStartTime, windowTotal))  ) =>
      val processDuration = curr - processStart
      val windowDuration = end - windowStartTime

      val overrallRate = if(processDuration == 0) 0 else  (processTotal.toDouble  /   processDuration) * 1000
      val windowRate = if(windowDuration == 0) 0 else  (windowTotal.toDouble /  windowDuration) * 1000
      s"windowRate $windowRate windowDurationInMillis ${windowDuration} overallRate $overrallRate processDurationInSeconds ${processDuration / 1000}" +
        s" totalRecords $processTotal"


  }




}

import tools.Window._

class Window(windowSize: FiniteDuration, sendOutput: Tuple2[ProcessStatsWithCurrTime, WindowStatsWithEndTime] => Unit)(implicit ec: ExecutionContext) extends Actor{

  case object Tick

  def receive = waitingForFirstContact

  def collecting(proccesStats: ProcessStats, windowStats: WindowStats): Receive = {

    case Report =>
      sendOutput(withCurrTime(System.currentTimeMillis, proccesStats, windowStats))

    case Tick =>

      val currTime = System.currentTimeMillis()
/*      println(s"window actor in collecting::tick Window duration was ${currTime - windowStats.startTime}." +
        s" currTime is $currTime, windowStats startTime is ${windowStats.startTime}, windowSize (in millis) is ${windowSize.toMillis}")*/

      //Looks like we can sometimes get a tick from a Cancellable that somehow hasn't been cancelled,
      //even though we think we cancel it in all the right places.
      //So let's not send output and schedule a new tick unless  we know we haven't been awakened by
      //a tick sent from a rogue Cancellable.
      if(currTime - windowStats.startTime >= windowSize.toMillis) {

        sendOutput(withCurrTime(currTime, proccesStats, windowStats))


        context.become(collecting(proccesStats, WindowStats(System.currentTimeMillis, 0)))

        scheduleNextTicK

      }




    case sizeOfIncrement: Long =>

      val currTime = System.currentTimeMillis

      if(currTime - windowStats.startTime >= windowSize.toMillis) {
/*        println(s"window actor in collecting::Long about to send output. Window duration was ${currTime - windowStats.startTime}." +
        s" currTime is $currTime, windowStats startTime is ${windowStats.startTime}, windowSize (in millis) is ${windowSize.toMillis}")*/


        val latestProcessStats = proccesStats.update(sizeOfIncrement)
        sendOutput(withCurrTime(currTime, latestProcessStats, windowStats.update(sizeOfIncrement)))

        context.become(
          collecting(latestProcessStats, WindowStats(System.currentTimeMillis(), 0))
        )


        scheduleNextTicK

      }
      else
        context.become(collecting(
          proccesStats.update(sizeOfIncrement),
          windowStats.update(sizeOfIncrement)
        )
        )





  }

  def waitingForFirstContact: Receive = {
    case sizeOfIncrement: Long =>
      //println(s"window actor in waitingForFirstContact sees $sizeOfIncrement")

      val currTime = System.currentTimeMillis()

      context.become( collecting(ProcessStats(currTime, sizeOfIncrement   ), WindowStats(currTime, sizeOfIncrement))  )

      scheduleNextTicK

  }

  def withCurrTime(curr: Long, processStats: ProcessStats, windowStats: WindowStats): (ProcessStatsWithCurrTime, WindowStatsWithEndTime) = {
    ( ProcessStatsWithCurrTime(curr, processStats), WindowStatsWithEndTime(curr, windowStats)   )
  }


  var nextScheduledTick: Option[Cancellable] = None

  def scheduleNextTicK = {
    nextScheduledTick.map{tickScheduler =>
      tickScheduler.cancel()
      //println("cancelled obsolete tick")
    }

    nextScheduledTick = Some(
      context.system.scheduler.scheduleOnce(windowSize, self, Tick)
    )
  }



}
