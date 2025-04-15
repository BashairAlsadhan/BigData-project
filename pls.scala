import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(5))
val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)
wordCounts.print()
ssc.start()

PS C:\Users\Royna\Documents\BigData\BigData-project> python -c "import socket, time; s=socket.socket(); s.bind(('localhost', 9999)); s.listen(1); print('Socket open on port 9999'); conn, _=s.accept(); print('Client connected'); [conn.send((input('> ') + '\n').encode()) or time.sleep(1) for _ in range(1000)]"
