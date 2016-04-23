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
    val i_opts = new IteratorSource.Options
    i_opts.iter = rdd_data
    val iteratorSource = new IteratorSource(i_opts)
    val learner = new Learner(iteratorSource, l.model, l.mixins, l.updater, l.datasink)
    learner.firstPass(null)
    learner.datasource.close
    learner.model.mats = null
    learner.model.gmats = null
    println("Learner: "+ SizeEstimator.estimate(learner));

    Iterator[Learner](learner)
  }

  // Runs subsequent passes of the learner.
  def nextPass(learner: Learner)(data_iterator: Iterator[(SerText, BIDMat.MatIO)]):Iterator[Learner] = {
    learner.datasource.asInstanceOf[IteratorSource].opts.iter = data_iterator
    learner.datasource.init
    learner.model.bind(learner.datasource)
    learner.nextPass(null)
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

  def runOnSpark(sc: SparkContext, learner:Learner, rdd_data:RDD[(SerText,MatIO)], num_partitions: Int):Learner = {
    // Instantiate a learner, run the first pass, and reduce all of the learners' models into one learner.
    var reduced_learner = rdd_data.mapPartitions[Learner](firstPass(learner), preservesPartitioning=true)
      .treeReduce(reduce_fn(0), 2)
    // Once we've reduced our distributed learners into one learner, we can update our model.
    reduced_learner.updateM(0)

    // While we still have more passes to complete
    for (i <- 1 until learner.opts.npasses) {
      // Call nextPass on each learner and reduce the learners into one learner
      val t0 = System.nanoTime()
      reduced_learner = rdd_data.mapPartitions[Learner](nextPass(reduced_learner), preservesPartitioning=true)
        .treeReduce(reduce_fn(i), 2)
      val t1 = System.nanoTime()
      println("Elapsed time iter " + i + ": " + (t1 - t0)/math.pow(10, 9)+ "s")
      // Update the model
      reduced_learner.updateM(i)
    }
    reduced_learner
  }
}
