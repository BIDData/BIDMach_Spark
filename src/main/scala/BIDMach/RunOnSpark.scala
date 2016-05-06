package BIDMach

import BIDMach.datasources.IteratorSource
import BIDMach.datasources.IteratorSource.{Options => IteratorOpts}
import BIDMach.models.Model
import BIDMach.updaters.Batch
import BIDMat.{MatIO, SerText}
import BIDMat.SciFunctions._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import org.apache.spark.util.SizeEstimator;
import BIDMach.models.KMeans

object RunOnSpark{
  // Called copy the reduced learner into a RDD of learners.
  def mapToLearner(learner:Learner)(l: Iterator[Int]): Iterator[Learner] = {
    Iterator[Learner](learner)
  }

  // Instantiates a learner based on the parameters of the learner that is passed in and binds the data to the new learner.
  def firstPass(l: Learner)(rdd_data:Iterator[(SerText, BIDMat.MatIO)]):Iterator[Learner] = {
    import BIDMat.{CMat,CSMat,DMat,Dict,FMat,FND,GMat,GDMat,GIMat,GLMat,GSMat,GSDMat,GND,HMat,IDict,Image,IMat,LMat,Mat,SMat,SBMat,SDMat}
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
    println(System.getProperty("java.class.path"))
    Mat.checkMKL(true)
    Mat.hasCUDA = 0
    Mat.checkCUDA(true)
    println("Mat hascuda: " + Mat.hasCUDA)

    if (!rdd_data.hasNext) {
      println("rdd_data is null!")
    }
    val i_opts = new IteratorSource.Options
    i_opts.iter = rdd_data
    val iteratorSource = new IteratorSource(i_opts)
    val learner = new Learner(iteratorSource, l.model, l.mixins, l.updater, l.datasink)
    learner.firstPass(null)
    if (learner.useGPU) {
      Learner.toCPU(learner.modelmats)
    }
    learner.datasource.close
    learner.model.mats = null
    learner.model.gmats = null
    println("Learner: "+ SizeEstimator.estimate(learner));

    Iterator[Learner](learner)
  }

  // Runs subsequent passes of the learner.
  def nextPass(l: Learner)(data_iterator: Iterator[(SerText, BIDMat.MatIO)], learner_iterator: Iterator[Learner]):Iterator[Learner] = {
    import BIDMat.{CMat,CSMat,DMat,Dict,FMat,FND,GMat,GDMat,GIMat,GLMat,GSMat,GSDMat,GND,HMat,IDict,Image,IMat,LMat,Mat,SMat,SBMat,SDMat}
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
    Mat.hasCUDA = 0
    Mat.checkCUDA(true)

    val learner = learner_iterator.next
    learner.model.copyFrom(l.model)
    learner.datasource.asInstanceOf[IteratorSource].opts.iter = data_iterator
    learner.datasource.init
    learner.model.bind(learner.datasource)
    learner.nextPass(null)
    if (learner.useGPU) {
      Learner.toCPU(learner.modelmats)
    }
    learner.datasource.close
    learner.model.mats = null
    learner.model.gmats = null
    println("Learner: "+ SizeEstimator.estimate(learner));
    Iterator[Learner](learner)
  }

  def wrapUpLearner(learner_iterator: Iterator[Learner]): Iterator[Learner] = {
    val learner = learner_iterator.next
    learner.wrapUp
    Iterator[Learner](learner)
  }

  // Gathers and reduces all the learners into one learner.
  def reduce_fn(ipass: Int)(l: Learner, r: Learner): Learner = {
    l.model.combineModels(ipass, r.model)
    l
  }

  def runOnSpark(sc: SparkContext, learner:Learner, rdd_data:RDD[(SerText,MatIO)], num_partitions: Int):Array[Learner] = {
    // Instantiate a learner, run the first pass, and reduce all of the learners' models into one learner.
    var rdd_learner: RDD[Learner] = rdd_data.mapPartitions[Learner](firstPass(learner), preservesPartitioning=true).persist()
    var reduced_learner = rdd_learner.treeReduce(reduce_fn(0), 2)
    // Once we've reduced our distributed learners into one learner, we can update our model.
    reduced_learner.updateM(0)

    // While we still have more passes to complete
    for (i <- 1 until learner.opts.npasses) {
      // Call nextPass on each learner and reduce the learners into one learner
      val t0 = System.nanoTime()
      val tmp_learner = rdd_data.zipPartitions(rdd_learner, preservesPartitioning=true)(nextPass(reduced_learner)).persist()

      reduced_learner = tmp_learner.treeReduce(reduce_fn(i), 2)
      rdd_learner.unpersist()
      rdd_learner = tmp_learner
      val t1 = System.nanoTime()
      println("Elapsed time iter " + i + ": " + (t1 - t0)/math.pow(10, 9)+ "s")
      // Update the model
      reduced_learner.updateM(i)
    }
    // Note: The returned RDD has a transformation applied to it.
    rdd_learner = rdd_learner.mapPartitions(wrapUpLearner, preservesPartitioning=true)
    rdd_learner.collect()
  }
}
