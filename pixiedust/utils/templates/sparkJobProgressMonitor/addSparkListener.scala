import org.apache.spark.sql.types._
import org.apache.spark.scheduler._
import org.apache.spark._
import org.apache.spark.TaskEndReason
import org.apache.spark.JobExecutionStatus
import org.apache.spark.SparkContext
import org.apache.spark.executor._
//import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage._
//import org.apache.spark.util.{Utils, JsonProtocol}
import org.apache.spark.scheduler.cluster._
//import org.apache.spark.InternalAccumulator

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.{ HashMap, HashSet, LinkedHashMap, ListBuffer }
import java.net._
import java.io._
import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.ibm.pixiedust._;

val executorCores = new HashMap[String, Int]
@volatile var totalCores: Int = 0
@volatile var numExecutors: Int = 0

def serializeStageJSON(stageInfo: StageInfo):String={
    return s"""{
        "stageId":"${stageInfo.stageId}",
        "name":"${stageInfo.name}",
        "details":"${stageInfo.details.replaceAll("\n", "\\\\\\\\n")}",
        "numTasks":${stageInfo.numTasks}
    }"""
}

def serializeStagesJSON(stageInfos: Seq[StageInfo]):String = {
    try{
        return "[" + stageInfos.sortWith( (s,t) => s.stageId < t.stageId ).map(si => serializeStageJSON(si)).reduce( _ + "," + _ ) + "]"
    }catch{
        case e:Throwable=>{
            e.printStackTrace();
            return "[]"
        }
    }
}

def serializeTaskStartJSON(taskInfo: TaskInfo):String = {
    return s"""{
        "taskId":"${taskInfo.taskId}", 
        "attemptNumber":${taskInfo.attemptNumber},
        "index":${taskInfo.index},
        "launchTime":${taskInfo.launchTime},
        "executorId":"${taskInfo.executorId}",
        "host":"${taskInfo.host}"
    }"""
}

def serializeTaskEndJSON(taskEnd: SparkListenerTaskEnd):String = {
    var taskInfo = taskEnd.taskInfo
    var metricsOpt = Option(taskEnd.taskMetrics)
    return s"""{
        "taskId":"${taskInfo.taskId}", 
        "attemptNumber":${taskInfo.attemptNumber},
        "index":${taskInfo.index},
        "launchTime":${taskInfo.launchTime},
        "executorId":"${taskInfo.executorId}",
        "host":"${taskInfo.host}",
	      "peakExecutionMemory":"${metricsOpt.map(_.peakExecutionMemory).getOrElse(0L)}"
    }"""
}

val __pixiedustSparkListener = new SparkListener{
    private val channelReceiver = new ChannelReceiver()

    def setChannelListener(listener:PixiedustOutputListener){
		channelReceiver.setChannelListener(listener)
	}

    override def onJobStart(jobStart: SparkListenerJobStart) {
        channelReceiver.send("jobStart", s"""{
            "jobId":"${jobStart.jobId}",
            "stageInfos":${serializeStagesJSON(jobStart.stageInfos)}
        }
        """)
    }
    override def onJobEnd(jobEnd: SparkListenerJobEnd){
        val jobResult = jobEnd.jobResult match{
            case JobSucceeded => "Success"
            case _ => "Failure"
        }
        channelReceiver.send("jobEnd", s"""{
            "jobId":"${jobEnd.jobId}",
            "jobResult": "${jobResult}"
        }
        """)
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart) {
        
        channelReceiver.send("taskStart", s"""{
            "stageId":"${taskStart.stageId}",
            "taskInfo":${serializeTaskStartJSON(taskStart.taskInfo)}
        }
        """)
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {

        channelReceiver.send("taskEnd", s"""{
            "stageId":"${taskEnd.stageId}",
            "taskType":"${taskEnd.taskType}",
            "taskInfo":${serializeTaskEndJSON(taskEnd)}
        }
        """)
    }

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) { 
        channelReceiver.send("stageSubmitted", s"""{
            "stageInfo": ${serializeStageJSON(stageSubmitted.stageInfo)}
        }
        """)
    }

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) { 
        //channelReceiver.send("taskGettingResult", s"taskGettingResult ${taskGettingResult.taskInfo.taskId} : ${taskGettingResult.taskInfo.executorId}")
    }

    //override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) { 
        //System.out.println(s"MetricsUpdate ${executorMetricsUpdate.execId} : ${executorMetricsUpdate.taskMetrics}")
    //}

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) { 
        channelReceiver.send("stageCompleted", s"""{
            "stageInfo":${serializeStageJSON(stageCompleted.stageInfo)}
        }
        """)
    }
    
    /** Called when an executor is added. */
    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) {
        executorCores(executorAdded.executorId) = executorAdded.executorInfo.totalCores
        totalCores += executorAdded.executorInfo.totalCores
        numExecutors += 1

        var executorInfoJson = s"""{
            "host" : "${executorAdded.executorInfo.executorHost}",
            "numCores" : "${executorAdded.executorInfo.totalCores}",
            "totalCores" : "${totalCores}"
        }"""

        channelReceiver.send("executorAdded", s"""{
            "executorId":"${executorAdded.executorId}",
            "time" : "${executorAdded.time}",
            "executorInfo" : "${executorInfoJson}"
        }
        """)
    }

    /** Called when an executor is removed. */
    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) {
        totalCores -= executorCores.getOrElse(executorRemoved.executorId, 0)
        numExecutors -= 1

        var executorInfoJson = s"""{
            "totalCores" : "${totalCores}"
        }"""

        channelReceiver.send("executorRemoved", s"""{
            "executorId":"${executorRemoved.executorId}",
            "time" : "${executorRemoved.time}",
            "executorInfo" : "${executorInfoJson}"
        }
        """)	    
    }

   override def onExecutorMetricsUpdate(metricsUpdate: SparkListenerExecutorMetricsUpdate) {
	val executorId = metricsUpdate.execId
	val accumUpdates = metricsUpdate.accumUpdates
	//val executorMetrics = render(metricsUpdate.executorUpdates.map(executorMetricsToJson(_)))
	val executorMetrics = render(executorMetricsUpdateToJson(metricsUpdate))
        channelReceiver.send("executorMetricsUpdate", s"""{
            "executorId":"${executorId}",
            "executorMetricsInfo" : "${executorMetrics}"
        }	
        """)	   
   }
	
  def executorMetricsUpdateToJson(metricsUpdate: SparkListenerExecutorMetricsUpdate): JValue = {
    val execId = metricsUpdate.execId
    val accumUpdates = metricsUpdate.accumUpdates
    ("Event" -> "SparkListenerExecutorMetricsUpdate") ~
    ("Executor ID" -> execId) ~
    ("Metrics Updated" -> accumUpdates.map { case (taskId, stageId, stageAttemptId, updates) =>
      ("Task ID" -> taskId) ~
      ("Stage ID" -> stageId) ~
      ("Stage Attempt ID" -> stageAttemptId) ~
      ("Accumulator Updates" -> JArray(updates.map(accumulableInfoToJson).toList))
    })
  }

  def accumulableInfoToJson(accumulableInfo: AccumulableInfo): JValue = {
    val name = accumulableInfo.name
    ("ID" -> accumulableInfo.id) ~
    ("Name" -> name) ~
    ("Update" -> accumulableInfo.update.map { v => accumValueToJson(name, v) }) ~
    ("Value" -> accumulableInfo.value.map { v => accumValueToJson(name, v) }) ~
    //("Count Failed Values" -> accumulableInfo.countFailedValues) ~
    //("Metadata" -> accumulableInfo.metadata)
  }	
	
 def accumValueToJson(name: Option[String], value: Any): JValue = {
    if (name.exists(_.startsWith("internal.metrics."))) {
      value match {
        case v: Int => JInt(v)
        case v: Long => JInt(v)
        // We only have 3 kind of internal accumulator types, so if it's not int or long, it must be
        // the blocks accumulator, whose type is `java.util.List[(BlockId, BlockStatus)]`
        case v =>
          JArray(v.asInstanceOf[java.util.List[(BlockId, BlockStatus)]].asScala.toList.map {
            case (id, status) =>
              ("Block ID" -> id.toString) ~
              ("Status" -> blockStatusToJson(status))
          })
      }
    } else {
      // For all external accumulators, just use strings
      JString(value.toString)
    }
  }
	
  def blockStatusToJson(blockStatus: BlockStatus): JValue = {
    val storageLevel = storageLevelToJson(blockStatus.storageLevel)
    ("Storage Level" -> storageLevel) ~
    ("Memory Size" -> blockStatus.memSize) ~
    ("Disk Size" -> blockStatus.diskSize)
  }

  def storageLevelToJson(storageLevel: StorageLevel): JValue = {
    ("Use Disk" -> storageLevel.useDisk) ~
    ("Use Memory" -> storageLevel.useMemory) ~
    ("Deserialized" -> storageLevel.deserialized) ~
    ("Replication" -> storageLevel.replication)
  }
	
/*	
  def accumulablesToJson(accumulables: Traversable[AccumulableInfo]): JArray = {
    JArray(accumulables
        .filterNot(_.name.exists(accumulableBlacklist.contains))
        .toList.map(accumulableInfoToJson))
  }
*/

	
  /** Convert executor metrics to JSON. */
  /**
  def executorMetricsToJson(executorMetrics: ExecutorMetrics): JValue = {
    val metrics = ExecutorMetricType.metricToOffset.map { case (m, _) =>
      JField(m.asInstanceOf[String], executorMetrics.getMetricValue(m))
    }
    JObject(metrics.toSeq: _*)
  }
  */
}

sc.addSparkListener(__pixiedustSparkListener)
