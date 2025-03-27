// Import required Spark packages
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

// Read the dataset and remove the header
val data = sc.textFile("/Users/wassayefalkherb/Desktop/Semester 8/IT 462 Big data/cleaned_student_stress_factors.csv")
val header = data.first()
val rows = data.filter(_ != header).map(_.split(","))

// Transformations:

// Transformation 1: Categorize Students by Stress Level (Low, Moderate, High)
val categorized = rows.map(r => {
  val stress = Try(r.last.toInt).getOrElse(0)
  if (stress == 0) ("Low", 1)
  else if (stress == 1) ("Moderate", 1)
  else ("High", 1)
})

// Transformation 2: Filter Students with Poor Support + High Stress 
val vulnerableStudents = rows.filter(r => Try(r.last.toInt).getOrElse(0) == 2 && Try(r(16).toDouble).getOrElse(1.0) < 0.5)

// Transformation 3: Group Students by Course and Average Stress 
val stressByCourse = rows.map(r => (r(12), (Try(r.last.toInt).getOrElse(0), 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues { case (sum, count) => sum.toDouble / count }

// Transformation 4: Cluster Students Based on Lifestyle Archetypes (same as clustring but without using ML)
val lifestyleClusters = rows.map(r => {
  val sleep = Try(r(6).toDouble).getOrElse(0.0)
  val study = Try(r(13).toDouble).getOrElse(0.0)
  val support = Try(r(16).toDouble).getOrElse(0.5)
  val performance = Try(r(12).toDouble).getOrElse(0.5)
  val teacherRel = Try(r(14).toDouble).getOrElse(0.5)
  val peerPressure = Try(r(17).toDouble).getOrElse(0.5)

  val tag = (if (sleep < 0.5) "low_sleep_" else "high_sleep_") +
            (if (study > 0.5) "high_study_" else "low_study_") +
            (if (support < 0.5) "low_support_" else "high_support_") +
            (if (performance >= 0.6) "good_perf_" else "poor_perf_") +
            (if (teacherRel < 0.4) "poor_teacher_" else "good_teacher_") +
            (if (peerPressure > 0.6) "high_pressure" else "low_pressure")

  (tag, 1)
})

// Transformation 5: Rank Students by Sleep-to-Study Ratio 
val ratios = rows.map(r => {
  val sleep = Try(r(6).toDouble).getOrElse(0.0)
  val study = Try(r(13).toDouble).getOrElse(1.0)
  val ratio = if (study == 0) 0.0 else sleep / study
  (r, ratio)
})

// Actions:

// Action 1: Count Students in Each Stress Category 
val categoryCounts = categorized.reduceByKey(_ + _).collect()

// Action 2: Top 5 Courses with Highest Average Stress 
val top5Courses = stressByCourse.takeOrdered(5)(Ordering[Double].reverse.on(_._2))

// Action 3: Collect Sample Students with Balanced Life 
//val balancedStudents = rows.filter(r => Try(r(6).toInt).getOrElse(0) >= 7 && Try(r(13).toInt).getOrElse(0) >= 2 && Try(r(13).toInt).getOrElse(0) <= 4 && Try(r.last.toInt).getOrElse(0) <= 3).takeSample(false, 5)
val balancedStudents = rows.filter(r => Try(r(6).toDouble).getOrElse(0.0) >= 0.7 && Try(r(13).toDouble).getOrElse(0.0) >= 0.2 && Try(r(13).toDouble).getOrElse(0.0) <= 0.4 && Try(r.last.toDouble).getOrElse(0.0) <= 1.0 ).takeSample(false, 5)

// Action 4: Find a Correlation between Study -Academic Performance- and Stress 
val studyStressPairs = rows.map(r => (Try(r(13).toDouble).getOrElse(0.0), Try(r.last.toDouble).getOrElse(0.0))).collect()

// Action 5: Detect Shifts in Stress Level per Course Year (Weak, Average, Strong) 
val stressByYear = rows.map(r => {
  val performance = Try(r(12).toDouble).getOrElse(0.0)
  val stress = Try(r.last.toInt).getOrElse(0)
  val category = performance match {
    case p if p <= 0.2 => "Weak"
    case p if p <= 0.6 => "Average"
    case _ => "Strong"
  }
  (category, stress)
}).groupByKey().mapValues(vals => vals.sum.toDouble / vals.size).collect()

// Action 6: count students Based on lifestyle groups
val lifestyleCounts = lifestyleClusters.reduceByKey(_ + _).collect()

// Print the results

// Distribution of Students by Stress Level
println( "Stress Level Categories Count:")
categoryCounts.foreach { case (level, count) =>
  println(s" - $level: $count students")
}

// Top 5 Academic Performance Levels with Highest Average Stress
println("\n Top 5 Courses with Highest Average Stress:")
top5Courses.zipWithIndex.foreach { case ((course, avg), i) =>
  println(s" ${i + 1}. Course: $course → Avg Stress: ${avg.formatted("%.2f")}")
}

// Sample of Students with a Balanced Lifestyle
println("\n Sample of Students with Balanced Lifestyle (Sleep ≥ 7, Study 2–4, Stress ≤ 3):")
balancedStudents.foreach(r => println(r.mkString(", ")))

// OR

println("\nSample of Students with Balanced Lifestyle (Sleep, Study Load, Stress):")
balancedStudents.foreach { r =>
  println(s"Sleep: ${r(6)}, Study Load: ${r(13)}, Stress Level: ${r.last}")
}

// OR

val featureNames = header.split(",") 
// Print the feature names as a header row
println("\nSample of Students with Balanced Lifestyle:")
println(featureNames.mkString(", "))
balancedStudents.foreach(row => println(row.mkString(", ")))

// Sample of Study Load vs. Stress Level
println("\n Sample of Study Load vs. Stress Level:")
studyStressPairs.take(5).foreach { case (study, stress) =>
  println(s" - Study Load: $study → Stress Level: $stress")
}

// Average Stress Level by Academic Performance Category
println("\n Average Stress Level by Course Year:")
stressByYear.foreach { case (year, avg) =>
  println(s" - $year → Avg Stress: ${avg.formatted("%.2f")}")
}

// Total Students with High Stress and Poor Support
println(s"\nTotal Students with High Stress and Poor Support: ${vulnerableStudents.count()}")

// Most Frequent Lifestyle Clusters
println("\nTop 5 Most Common Lifestyle Clusters:")
lifestyleCounts.sortBy(-_._2).take(5).foreach { case (cluster, count) =>
  println(s" - $cluster: $count students")
}

//OR

// Show all lifestyle clusters
println("\n Lifestyle Clusters (based on Sleep, Study, Support):")
lifestyleCounts.foreach { case (cluster, count) =>
  println(s" - $cluster: $count students")
}