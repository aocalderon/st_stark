package dbis.stark.spatial.partitioner

import java.nio.file.Path

import dbis.stark.spatial.{Cell, NPoint, NRectRange, StarkUtils}
import dbis.stark.{Instant, Interval, STObject, TemporalExpression}
import org.apache.spark.rdd.RDD
import spire.ClassTag

object SpatioTempPartitioner {
  def apply[G <: STObject : ClassTag, T : ClassTag] (
    rdd: RDD[(G,T)], 
    cellSize: Double = 1.0, 
    spatial_capacity:Double = 1000,
    temporal_capacity: Double = 200,
    pointsOnly: Boolean = true,
    fraction: Double = 1.0
  ): SpatioTempPartitioner[G] = {

    val minmax = getMinMax(rdd)
    val spatialPartitioner = BSPartitioner (
      rdd, 
      cellSize, 
      spatial_capacity, 
      pointsOnly, 
      (minmax._1, minmax._2, minmax._3, minmax._4)
    )
    val partitions = spatialPartitioner.partitions.map(cell => (cell, Array(0L)))

    import TemporalRangePartitioner._

    val ST_Sample = rdd
      .sample(withReplacement = false, fraction = fraction)
      .map{ case(so, _) =>
        val start = so.getStart.get
        val sId = spatialPartitioner.getPartitionId(so)

        (sId, start)
      }
      .collect()
      .groupBy(_._1)
      .mapValues(_.map(_._2))

    ST_Sample.map{ case(sId, instants) =>
      (sId, instants.size)
    }.toList.sortBy(_._1)
    .map{ case(id, size) =>
      s"$id\t$size"
    }.foreach{println}
    
    ST_Sample
      .foreach{ case(sId, instants) =>
        val bins = countHistogram(instants)
        val groups = groupInstants(bins, temporal_capacity)

        val tempPartitions: Array[Long] = groups.map{ group =>
          group.last.instant
        }.sorted.toArray

        val sCell = spatialPartitioner.partitionBounds(sId)
        partitions(sId) = (sCell, 0L +: tempPartitions)
      }

    new SpatioTempPartitioner[G] (
      partitions, 
      minmax._1, minmax._2, minmax._3, minmax._4, 
      pointsOnly
    )
  }

  def getMinMax[G <: STObject, T](rdd:RDD[(G,T)]): (Double, Double, Double, Double, Long, Long) = {
    val (minX, maxX, minY, maxY, start, end) = rdd.map{ case (g,_) =>
      val env = g.getEnvelopeInternal
      val t = g.getTemp.get
      (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY, t.start.value, t.end.getOrElse(StarkUtils.MAX_LONG_INSTANT).value)

    }.reduce { (oldMM, newMM) =>
      val newMinX = oldMM._1 min newMM._1
      val newMaxX = oldMM._2 max newMM._2
      val newMinY = oldMM._3 min newMM._3
      val newMaxY = oldMM._4 max newMM._4

      val newStart = oldMM._5 min newMM._5
      val newEnd = oldMM._6 max newMM._6

      (newMinX, newMaxX, newMinY, newMaxY, newStart, newEnd)
    }

    // do +1 for the max values to achieve right open intervals
    (minX, maxX + GridPartitioner.EPS, minY, maxY + GridPartitioner.EPS, start, end)
  }
}

class SpatioTempPartitioner[G <: STObject : ClassTag] private (
  _partitions: Array[(Cell, Array[Long])],
  _minX: Double, _maxX: Double, _minY: Double, _maxY: Double,
  pointsOnly: Boolean ) extends GridPartitioner(_partitions.map(_._1), _minX, _maxX, _minX, _maxY) {

  def getSTBounds(idx: Int): (NRectRange, Array[Long]) = {
    require(0 <= idx && idx < _partitions.length, s"wrong index $idx. Not in [0 , ${_partitions.length}]")
    val (cell, intervals) = _partitions(idx)

    (cell.extent, intervals)
  }

  override def partitionBounds(idx: Int): Cell = partitions(idx)

  override def partitionExtent(idx: Int): NRectRange = partitions(idx).extent

  override def getPartitionId(key: Any): Int = {
    val g = key.asInstanceOf[G]
    val c = g.getGeo.getCentroid
    val p = NPoint(c.getX, c.getY)

    var i = 0
    var minSDist = 0.0
    var minDistId = -1
    var offset = 0
    while(i < partitions.length) {
      if(partitions(i).range.contains(p)) { // FOUND a spatial partition

        // now find a temporal partition in there that contains g's temp start
        val tID = TemporalRangePartitioner.getCellId(g.getTemp.get.start.value, _partitions(i)._2)

        // compute the partition ID and return
        return offset + tID

      } else { // the current spatial partition does not contain g

        offset += _partitions(i)._2.length // update offset: add current number of temp partitions

        // compute distance to nearest spatial partition - just in case no spatial partition will be found
        val d = partitions(i).range.dist(p)
        if(d < minSDist || i == 0){
          minSDist = d
          minDistId = i
        }
      }
      i += 1
    }

    /* IF WE GET HERE no spatial partition found that contains p
     * assign to the one with the smallest distance
     */
    // 1. Compute offset until the minDistId Partition...
    offset = _partitions.iterator.take(minDistId).map(_._2.length).sum
    // 2. Find temporal partition for g...
    val tId = TemporalRangePartitioner.getCellId(g.getTemp.get.start.value, _partitions(minDistId)._2)
    // 3. Return the according partition id...
    offset + tId
  }

  override def printPartitions(fName: Path): Unit = {
    val cubes = partitions.map{ cell =>
      val (envelope, intervals) = getSTBounds(cell.id)
      val wkt = envelope.wkt
      val tids = intervals.mkString("->")
      
      s"$wkt\t$tids"
    }
    GridPartitioner.writeToFile(cubes, fName)
  }

  override def numPartitions: Int = {
    var sum = 0
    var i = 0
    while(i < _partitions.length) {
      sum += _partitions(i)._2.length
      i += 1
    }
    sum
  }

  def numSpatialPartitions = _partitions.length
}
