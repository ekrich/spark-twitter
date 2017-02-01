package org.ekrich

case class Stats(sum: Double, sumOfSquares: Double, len: Int) {
  lazy val average = sum / len
  lazy val averageSquared = average * average
  lazy val variance = sumOfSquares / len - averageSquared
  lazy val stddev = Math.sqrt(variance)
  lazy val sampleVariance = sumOfSquares / (len - 1) - averageSquared
  lazy val sampleStddev = Math.sqrt(sampleVariance)
  def print() = println(f"""Length: $len Ave: $average%.2f Variance: $variance%.2f Std Dev: $stddev%.2f 
    Sample Variance: $sampleVariance%.2f Sample Std Dev: $sampleStddev%.2f""")
}



object Stats {
  import org.apache.spark.rdd.RDD
  
  /**
   * Spark entry point. Unfortunately, RDD and Scala collections
   * have not interfaces in common so we have to duplicate the processing
   * code.
   * <pre>
   * val rdd: RDD[Double] = ???
   * val stats = Stats.process(rdd)
   * </pre>
   */
  def process(numbers: RDD[Double]): Stats = {
    val mapRes = numbers.flatMap(x => List((x, x*x, 1)))                                         
    val res = mapRes.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))                                                
    tupled(res)
  }
  
  def process(numbers: List[Double]): Stats = {
    processRaw(numbers)
  }
  
  // needed to define since this is not the default object Stats {}
  def tupled(res: (Double, Double, Int)): Stats = {
    Stats(res._1, res._2, res._3)
  }
  
  private def processRaw(numbers: List[Double]): Stats = {
    val mapRes = numbers.flatMap(x => List((x, x*x, 1)))                                         
    val res = mapRes.reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))                                                
    tupled(res)
  }
  
}