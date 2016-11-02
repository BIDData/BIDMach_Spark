package BIDMach

import BIDMach.datasources.IteratorSource
import BIDMach.datasources.IteratorSource.{Options => IteratorOpts}
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
import BIDMat.{CMat, CSMat, DMat, Dict, FMat, FND, GDMat, GIMat, GLMat, GMat, GND, GSDMat, GSMat, HMat, IDict, IMat, Image, LMat, Mat, SBMat, SDMat, SMat}
import BIDMat.MatFunctions._
import BIDMat.SciFunctions._
import BIDMat.Solvers._
import BIDMat.Plotting._
import BIDMach.models.{Click, FM, GLM, KMeans, KMeansw, LDA, LDAgibbs, Model, NMF, RandomForest, SFA, SVD}
import BIDMach.networks.Net
import BIDMach.datasources.{DataSource, FileSource, MatSource, SFileSource}
import BIDMach.datasinks.{DataSink, MatSink}
import BIDMach.mixins.{CosineSim, L1Regularizer, L2Regularizer, Perplexity, Top}
import BIDMach.updaters.{ADAGrad, Batch, BatchNorm, Grad, IncMult, IncNorm, Telescoping}
import BIDMach.causal.IPTW
import java.net.{InetAddress, InetSocketAddress}

import BIDMach.allreduce.{Command, Master, Worker}

object RunOnSparkGLM {

  def runZipPartitions[A:ClassTag, B:ClassTag, V:ClassTag]
  (sc:SparkContext, rddA:RDD[A], rddB:RDD[B],f:(Iterator[A], Iterator[B]) => Iterator[V], preservesPartitioning:Boolean = true) = {
    val zipJob = rddA.zipPartitions(rddB, preservesPartitioning)(f).persist()
    sc.runJob(zipJob, (iter:Iterator[_]) => {})
  }

  def runWithBoundData(func:(Learner)=>Unit)(data_iter: Iterator[(Text, BIDMat.MatIO)], learn_iter: Iterator[Learner]) = {
    val learner = learn_iter.next

    learner.datasource.asInstanceOf[IteratorSource].opts.iter = data_iter
    learner.datasource.init
    learner.model.bind(learner.datasource)

    func(learner)

    learner.datasource.close
    learner.model.mats = null
    learner.model.gmats = null

    Iterator(null)
  }

  // def runOnSparkGLM(sc: SparkContext, learner:Learner, rddData:RDD[(Text,MatIO)], num_partitions: Int):Array[Learner] = {
  def runOnSparkGLM(sc: SparkContext, protoLearner: Learner, rddData: RDD[(Text, MatIO)], num_partitions: Int,
                    withKylix:Boolean = false) = {
    // Instantiate a learner, run the first pass, and reduce all of the learners' models into one learner.
    Mat.checkMKL(true)
    Mat.hasCUDA = 0
    Mat.checkCUDA(true)

    val workerCmdPorts = (0 until num_partitions).map(_ => scala.util.Random.nextInt(55532) + 10000 - 2)
    val LOCAL_IP = java.net.InetAddress.getLocalHost.getHostAddress

    val m = new Master();
    val opts = m.opts;
    opts.trace = 3;
    opts.intervalMsec = 2000;
    //opts.limitFctn = Master.powerLimitFctn
    opts.limit = 1000000
    opts.timeScaleMsec = 2e-3f
    opts.permuteAlways = false

    m.init

    val rddWorkerLearner: RDD[(Worker, Learner)] = rddData.mapPartitionsWithIndex(
      (idx: Int, data_iter: Iterator[(Text, BIDMat.MatIO)]) => {
        import BIDMat.{CMat, CSMat, DMat, Dict, FMat, FND, GMat, GDMat, GIMat, GLMat, GSMat, GSDMat, GND, HMat, IDict, Image, IMat, LMat, Mat, SMat, SBMat, SDMat}
        import BIDMat.MatFunctions._
        import BIDMat.SciFunctions._
        import BIDMat.Solvers._
        import BIDMat.Plotting._
        import BIDMach.Learner
        import BIDMach.models.{Click, FM, GLM, KMeans, KMeansw, LDA, LDAgibbs, Model, NMF, SFA, RandomForest, SVD}
        import BIDMach.networks.{Net}
        import BIDMach.datasources.{DataSource, MatSource, FileSource, SFileSource}
        import BIDMach.datasinks.{DataSink, MatSink}
        import BIDMach.mixins.{CosineSim, Perplexity, Top, L1Regularizer, L2Regularizer}
        import BIDMach.updaters.{ADAGrad, Batch, BatchNorm, Grad, IncMult, IncNorm, Telescoping}
        import BIDMach.causal.{IPTW}
        Mat.checkMKL(true)
        Mat.hasCUDA = 0
        Mat.checkCUDA(true)

        val l = protoLearner
        val i_opts = new IteratorSource.Options
        i_opts.iter = data_iter
        val iteratorSource = new IteratorSource(i_opts)
        val learner = new Learner(iteratorSource, l.model, l.mixins, l.updater, l.datasink)
        learner.init

        val worker = new Worker();
        val opts = worker.opts;
        val cmdPort = workerCmdPorts(idx);
        opts.trace = 4;
        opts.machineTrace = 1;
        opts.commandSocketNum = cmdPort + 0;
        opts.responseSocketNum = cmdPort + 1;
        opts.peerSocketNum = cmdPort + 2;

        learner.datasource.close
        learner.model.mats = null
        learner.model.gmats = null

        Iterator[(Worker, Learner)]((worker, learner))
      },
      preservesPartitioning = true
    ).persist()
    sc.runJob(rddWorkerLearner, (iter:Iterator[_]) => {})

    if (withKylix) {
      val workerAddrs: Array[InetSocketAddress] = rddWorkerLearner.mapPartitions((iter: Iterator[(Worker, Learner)]) => {
        val (worker, learner) = iter.next
        worker.start(learner)

        Iterator[InetSocketAddress](
          new InetSocketAddress(worker.workerIP.getHostAddress, worker.opts.commandSocketNum))
      }, preservesPartitioning = true).collect()

      val nmachines = workerAddrs.length;
      val gmods = irow(nmachines);
      val gmachines = irow(0 -> nmachines);
      m.config(gmods, gmachines, workerAddrs)
      m.setMachineNumbers
      m.sendConfig

      m.startUpdates() // start Kylix
    }

    val rddLearner: RDD[Learner] = rddWorkerLearner.values.persist()

    val t0 = System.nanoTime()
    runZipPartitions(sc, rddData, rddLearner, runWithBoundData((learner:Learner) => {
      learner.init
      learner.firstPass(null)
    }))
    val t1 = System.nanoTime()
    println("Elapsed time iter 0: " + (t1 - t0) / math.pow(10, 9) + "s")

    // While we still have more passes to complete
    for (iter <- 1 until protoLearner.opts.npasses) {
      val t0 = System.nanoTime()

      runZipPartitions(sc, rddData, rddLearner, runWithBoundData((learner:Learner) => {
        learner.nextPass(null)
      }))

      val t1 = System.nanoTime()
      println("Elapsed time iter " + iter + ": " + (t1 - t0) / math.pow(10, 9) + "s")
    }

    if (withKylix) {
      m.stopUpdates() // stop Kylix
    }

    rddLearner.foreachPartition((iter: Iterator[Learner]) => iter.next.wrapUp(protoLearner.opts.npasses - 1))
    rddLearner.collect()
  }
}
