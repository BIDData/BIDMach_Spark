package BIDMach

import BIDMach.datasources.IteratorSource
import BIDMach.datasources.IteratorSource.{Options => IteratorOpts}
import BIDMach.datasources.ArraySource
import BIDMach.datasources.ArraySource.{Options => ArrayOpts}
import BIDMach.models.Model
import BIDMach.updaters.Batch
import BIDMat.MatIO
import BIDMat.SciFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.spark.util.SizeEstimator
import BIDMach.models.KMeans
import BIDMat.{CMat,CSMat,DMat,Dict,FMat,FND,GMat,GDMat,GIMat,GLMat,GSMat,GSDMat,GND,HMat,IDict,
               Image,IMat,LMat,Mat,SMat,SBMat,SDMat}
import BIDMat.MatFunctions._
import BIDMat.SciFunctions._
import BIDMat.Solvers._
import BIDMat.Plotting._
import BIDMach.models.{Click,FM,GLM,KMeans,KMeansw,LDA,LDAgibbs,Model,NMF,SFA,RandomForest,SVD}
import BIDMach.networks.{Net}
import BIDMach.datasources.{DataSource,MatSource,FileSource,SFileSource}
import BIDMach.datasinks.{DataSink,MatSink}
import BIDMach.mixins.{CosineSim,Perplexity,Top,L1Regularizer,L2Regularizer}
import BIDMach.updaters.{ADAGrad,Batch,BatchNorm,Grad,IncMult,IncNorm,Telescoping}
import BIDMach.causal.{IPTW}
import java.net.{InetAddress,InetSocketAddress}
import BIDMach.allreduce.{Master,Worker,Command}

object RunOnSparkGLM {

  // def runOnSparkGLM(sc: SparkContext, learner:Learner, rddData:RDD[(Text,MatIO)], numPartitions: Int):Array[Learner] = {
  def runOnSparkGLM(sc: SparkContext, protoLearner:Learner, rddData:RDD[(Text,MatIO)], numPartitions: Int) = {
    // Instantiate a learner, run the first pass, and reduce all of the learners' models into one learner.
    Mat.checkMKL(true)
    // Mat.hasCUDA = 0
    Mat.checkCUDA(true)

    val masterIP = java.net.InetAddress.getLocalHost
    val LOCAL_IP = masterIP.getHostAddress

    val m = new Master()
    val opts = m.opts
    opts.trace = 3
    opts.intervalMsec = 2000
    opts.limit = 1000000
    opts.timeScaleMsec = 2e-3f
    opts.permuteAlways = false

    opts.machineThreshold = 0.75
    opts.minWaitTime = 3000
    opts.timeThresholdMsec = 5000

    val masterCommAddr = new InetSocketAddress(
      masterIP,
      m.opts.responseSocketNum)

    m.init
    m.startUpdatesAfterRegistration(numPartitions)

    val models = rddData.mapPartitions(
      (dataIter:Iterator[(Text, MatIO)]) => {
        import BIDMat.{CMat,CSMat,DMat,Dict,FMat,FND,GMat,GDMat,GIMat,GLMat,GSMat,GSDMat,GND,HMat,
                       IDict,Image,IMat,LMat,Mat,SMat,SBMat,SDMat}
        import BIDMat.MatFunctions._
        import BIDMat.SciFunctions._
        import BIDMat.Solvers._
        import BIDMat.Plotting._
        import BIDMach.Learner
        import BIDMach.models.{Click,FM,GLM,KMeans,KMeansw,LDA,LDAgibbs,Model,NMF,SFA,RandomForest,SVD}
        import BIDMach.networks.{Net}
        import BIDMach.datasources.{DataSource,MatSource,FileSource,SFileSource}
        import BIDMach.datasinks.{DataSink,MatSink}
        import BIDMach.mixins.{CosineSim,Perplexity,Top,L1Regularizer,L2Regularizer}
        import BIDMach.updaters.{ADAGrad,Batch,BatchNorm,Grad,IncMult,IncNorm,Telescoping}
        import BIDMach.causal.{IPTW}
        Mat.checkMKL(true)
        Mat.checkCUDA(true)

        val l = protoLearner
        val iopts = new ArraySource.Options
        val arraySource = new ArraySource(iopts)
        val learner = new Learner(arraySource, l.model, l.mixins, l.updater, l.datasink)

        val worker = new Worker()
        val wopts = worker.opts
        wopts.trace = 4
        wopts.machineTrace = 1
        val basePort = scala.util.Random.nextInt(55530) + 10000
        wopts.commandSocketNum = basePort
        wopts.responseSocketNum = basePort + 1
        wopts.peerSocketNum = basePort + 2

        worker.start(learner)
        worker.registerWorker(masterCommAddr)

        val dataArr = dataIter.toArray

        learner.datasource.asInstanceOf[ArraySource].opts.dataArray = dataArr
        learner.datasource.init
        learner.model.bind(learner.datasource)
        learner.init
        for (iter <- 0 until protoLearner.opts.npasses) {
          if (iter == 0) {
            learner.firstPass(null)
          } else {
            learner.nextPass(null)
          }
          learner.datasource.reset
        }
        learner.wrapUp(protoLearner.opts.npasses - 1)

        worker.signalLearnerDone
        worker.stop
        worker.shutdown
        worker.executor = null  // make sure we can serialize

        Iterator[Model](learner.model)
      },
      preservesPartitioning=true
    ).collect()

    m.stopUpdates

    (m, models)
  }
}
