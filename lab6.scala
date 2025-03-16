import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Step 1: Initialize SparkSession
val spark = SparkSession.builder.appName("StudentsDF").master("local[*]").getOrCreate()
import spark.implicits._

// Step 2: Create a case class
case class Student(StudentName: String, TotalGrades: Double, isPass: Boolean)

// Step 3: Create a DataFrame manually
val studentsDF = Seq(
  Student("Ali", 85, true),
  Student("Sara", 92, true),
  Student("Ahmed", 45, false),
  Student("Huda", 78, true),
  Student("Faisal", 88, true),
  Student("Noor", 35, false),
  Student("Omar", 91, true),
  Student("Laila", 50, false)
).toDF()

// Step 4: Show the DataFrame
studentsDF.show()

// 1. Get the average grades of students who failed
val avgFailed = studentsDF.filter($"isPass" === false).agg(avg("TotalGrades").alias("Avg_Failed"))
avgFailed.show()

// 2. Get the average grades of students who passed
val avgPassed = studentsDF.filter($"isPass" === true).agg(avg("TotalGrades").alias("Avg_Passed"))
avgPassed.show()

// 3. Get both average grades in the same table using groupBy
val avgGrades = studentsDF.groupBy("isPass").agg(avg("TotalGrades").alias("Average_Grade"))
avgGrades.show()

// 4. Get the students whose grade is above 90
val topStudents = studentsDF.filter($"TotalGrades" > 90)
topStudents.show()

// Stop SparkSession
spark.stop()
