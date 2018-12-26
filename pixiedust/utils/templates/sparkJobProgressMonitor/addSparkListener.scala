import org.apache.spark.scheduler._
import org.apache.spark.sql.types._
import collection.JavaConverters._

import org.apache.spark.scheduler._
import org.apache.spark._
import org.apache.spark.TaskEndReason
import org.apache.spark.JobExecutionStatus
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.apache.spark.scheduler.cluster._
import scala.collection.mutable.{ HashMap, HashSet, LinkedHashMap, ListBuffer }
import java.net._
import java.io._

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

    override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) { 
        //System.out.println(s"MetricsUpdate ${executorMetricsUpdate.execId} : ${executorMetricsUpdate.taskMetrics}")
    }

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
        channelReceiver.send("executorAdded", s"""{
            "executorId":"${executorAdded.executorId}",
            "time" : "${executorAdded.time}",
            "host" : "${executorAdded.executorInfo.executorHost}",
            "numCores" : "${executorAdded.executorInfo.totalCores}",
            "totalCores" : "${totalCores}"
        }
        """)
    }

    /** Called when an executor is removed. */
    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) {
        totalCores -= executorCores.getOrElse(executorRemoved.executorId, 0)
        numExecutors -= 1

        // println("SPARKMONITOR_LISTENER: Executor Removed: \n" + pretty(render(json)) + "\n")
        channelReceiver.send("executorRemoved", s"""{
            "executorId":"${executorRemoved.executorId}",
            "time" -> "${executorRemoved.time}",
            "totalCores" : "${totalCores}"
        }
        """)
    }

}

sc.addSparkListener(__pixiedustSparkListener)
