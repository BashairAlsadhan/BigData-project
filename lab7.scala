import org.apache.spark._
import org.apache.spark.streaming._


val ssc = new StreamingContext(sc, Seconds(10))

val lines = ssc.textFileStream("test/")

val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x=>(x,1)).reduceByKey(_ + _)
wordCounts.print()

ssc.start()
ssc.awaitTermination()