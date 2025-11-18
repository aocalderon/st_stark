package dbis.stark.spatial.partitioner

import dbis.stark.spatial.StarkUtils
import dbis.stark.{Instant, Interval, STObject, TemporalExpression}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import com.fasterxml.jackson.module.scala.deser.overrides

/**
  * Created by Jacob on 20.02.17.
  */


object TemporalRangePartitioner {

  def getCellId(start: Long, cells: Array[Long]): Int = {

    var i = 0
    while(i < cells.length) {
      if (cells(i) > start) {
        return i - 1
      }

      i += 1
    }

    cells.length - 1
  }

  def getCellId(start: Long, minT: Long, maxT: Long, partitions: Int): Int = {

    val length = (maxT * 1.3) - minT
    val interval = length / partitions

    val position_in_range = start - minT

    val res = (position_in_range / interval).toInt

    lazy val s = " l: " + length + "  int: " + interval + " pos: " + position_in_range

    require(res >= 0 && res < partitions, s"Cell ID out of bounds (0 .. $partitions): $res " + " object:" + start + s)
    res
  }

  /**
    * A bin of a histogram that count how many entries per instant in a sequence.
    *
    * @param instant A particular instant.
    * @param count The count of entries in this instant.
    */
  case class Bin(instant: Long, count: Int){
    override def toString(): String = s"[$instant, $count]"
  }

  /**
    * Count the histogram of a sequence of instants.
    */
  def countHistogram(sample: IndexedSeq[Instant]): Seq[Bin] = {
    sample.map{ instant =>
      val t = instant.value
      Bin(t, 1)
    }
    .groupBy(_.instant)
    .mapValues(_.map(_.count)
    .reduce(_ + _))
    .map{ case(t, c) => Bin(t, c) }
    .toSeq
    .sortBy(_.instant)
  }

  /**
   * Groups a sequence of bins (instants and their counts) into sub-sequences from left to right, 
   * such that the sum of each group does not exceed a maximum limit.  
   * The process is greedy: it maximizes the size of the current group before starting a new one. 
   * The relative order of elements is preserved.
   *
   * @param numbers The input sequence of integers.
   * @param maxSum The maximum allowed sum for any group.
   * @return A list of lists, where each inner list is a valid group (except for elements that 
   * individually exceed the maxSum, which are handled with a warning).
   */
  def groupInstants(numbers: Seq[Bin], maxSum: Double): List[List[Bin]] = {
    // We use a foldLeft to process the list sequentially and maintain state.
    // The state (accumulator) is a tuple: (completedGroups, currentGroup, currentSum)
    val initialState = (List.empty[List[Bin]], List.empty[Bin], 0)

    val (completedGroups, finalGroup, _) = numbers.foldLeft(initialState) {
      case ((groups, currentGroup, currentSum), bin) =>
        
        // 1. Sanity Check: If an individual number exceeds the maximum sum,
        // it must form its own group. We complete the current group and start 
        // a new one containing only the violating number.
        val number = bin.count
        if (number > maxSum) {
           //println(s"Warning: Element $number exceeds maxSum $maxSum. It will be placed in a separate group.")
           val updatedGroups = if (currentGroup.nonEmpty) groups :+ currentGroup else groups
           // Start a new group with the single, violating number and immediately complete it.
           (updatedGroups :+ List(bin), List.empty[Bin], 0)
        }
        
        // 2. Standard case: Check if adding the number is within the limit
        else if (currentSum + number <= maxSum) {
          // If the sum is within the limit, add the number to the current group
          // Note: using :+ on List for simple append is fine for small lists, 
          // but for highly performance-critical code on very large lists, 
          // one might prefer prepending and reversing at the end.
          (groups, currentGroup :+ bin, currentSum + number)
        } else {
          // 3. Limit exceeded: The current group is complete.
          // Start a new group with the current number.
          (groups :+ currentGroup, List(bin), number)
        }
    }

    // After folding, we must add the last group being built (finalGroup), if it's not empty.
    if (finalGroup.nonEmpty) completedGroups :+ finalGroup else completedGroups
  }

  def maximumRange(minT: Long, maxT: Long, numPartitions: Int, 
    sample: IndexedSeq[Instant], capacity: Int = 50, spatialID: Int = 0): Array[Long] = {

    val bins = countHistogram(sample)
    val groups = groupInstants(bins, capacity)
    //println(s"Intervals:\t ${spatialID}")
    //println(s"Bins:\t ${bins.sortBy(_.instant).mkString(" - ")}")
    //println(groups.sortBy(_.head.instant).map{_.map{_.instant}.mkString(",")}.mkString(";"))

    val arr = new Array[Long](numPartitions)
    val range = maxT - minT
    val dist = Math.round(range / numPartitions)

    arr(0) = 0

    var i = 1
    while(i < arr.length) {
      arr(i) = minT + i * dist
      i += 1
    }

    arr
  }

  def autoRange(sample: IndexedSeq[Instant], numPartitions: Int, spatialID: Int = 0): Array[Long] = {
    val sorted = sample.sortBy(_.value)
    val maxitems = Math.round(sorted.length / numPartitions)

    val arr = new Array[Long](numPartitions)
    arr(0) = 0

    var i = 1
    while(i < arr.length) {
      arr(i) = sorted(i * maxitems).value
      i += 1
    }

    arr
  }

  def fixedRange(minT: Long, maxT: Long, numPartitions: Int): Array[Long] = {
    val arr = new Array[Long](numPartitions)
    val range = maxT - minT
    val dist = Math.round(range / numPartitions)

    // FIXME is 0 here always correct? better Long.MIN_VALUE?
    arr(0) = 0

    var i = 1
    while(i < arr.length) {
      arr(i) = minT + i * dist
      i += 1
    }

    arr
  }

}


class TemporalRangePartitioner[G <: STObject : ClassTag, V: ClassTag](rdd: RDD[(G, V)],
                                                                      partitions: Int, autoRange: Boolean,
                                                                       _minT: Long, _maxT: Long, sampelsize: Double, instantsOnly: Boolean = false)
  extends TemporalPartitioner(_minT, _maxT) {



  //new Array[Long](partitions)
  private val cells: Array[Long] = {

    val array = if (autoRange) {
      val sample = rdd.sample(withReplacement = false, sampelsize).map(x => x._1.getTemp.get.start).collect()
      TemporalRangePartitioner.autoRange(sample, partitions)
    } else {
      TemporalRangePartitioner.fixedRange(minT,maxT,partitions)
    }

    array

  }

  val bounds: Array[Long] = if(instantsOnly) cells else {
    val arr = new Array[Long](partitions)
    rdd.map { case (g, _) =>
      val end = g.getTemp.get.end.getOrElse(StarkUtils.MAX_LONG_INSTANT).value
      val start = g.getTemp.get.start.value

      val id = TemporalRangePartitioner.getCellId(start, cells)

      //        println(s"$center --> $id")
      (id, end)
    }
      .reduceByKey { case (a, b) => if (a > b) a else b }
      .collect
      .foreach { case (id, end) =>
        arr(id) = end
      }
    arr
  }


  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean,
           minMax: (Long, Long),
           sampelsize: Double) = {
    this(rdd, partitions, autoRange, minMax._1, minMax._2, sampelsize)

  }

  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean,
           minMax: (Long, Long)) =
    this(rdd, partitions, autoRange, minMax, 0.01)

  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean
          ) =
    this(rdd, partitions, autoRange, TemporalPartitioner.getMinMax(rdd))

  def this(rdd: RDD[(G, V)],
           partitions: Int,
           autoRange: Boolean,
           sampelsize: Double
          ) =
    this(rdd, partitions, autoRange, if (autoRange) {
      (0, 0)
    } else {
      TemporalPartitioner.getMinMax(rdd)
    }, sampelsize)


  override def numPartitions: Int = partitions

  /**
    * Compute the partition for an input key.
    * In fact, this is a Geometry for which we use its centroid for
    * the computation
    *
    * @param key The key geometry to compute the partition for
    * @return The Index of the partition
    */
  override def getPartition(key: Any): Int = {
    val g = key.asInstanceOf[G]

    val start = g.getTemp.get.start.value

    var id = 0
    // if(autoRange){

    id = TemporalRangePartitioner.getCellId(start, cells)
    /*}else {
      id = TemporalRangePartitioner.getCellId(start, minT, maxT, partitions)
    }*/





    require(id >= 0 && id < numPartitions, s"Cell ID out of bounds (0 .. $numPartitions): $id " + " object:" + key)

    id
  }


  override def partitionBounds(idx: Int): TemporalExpression = {
    //println(bounds.mkString(" , "))
    Interval(cells(idx), bounds(idx))
  }
}
