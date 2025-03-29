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

// Transformation 3: Group Students by Academic Performance Score and Calculate Average Stress Level
val avgStressByPerformance = rows.map(r => (r(12), (Try(r.last.toInt).getOrElse(0), 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues { case (sum, count) => sum.toDouble / count }

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

// Transformation 6: Students with Balanced Life ( Sleep>= 7 hours, Study 2-4 hours, stress levell = 0)
val balancedCandidates = rows.filter(r =>
  Try(r(6).toDouble).getOrElse(0.0) >= 0.7 &&
  Try(r(13).toDouble).getOrElse(0.0) >= 0.2 &&
  Try(r(13).toDouble).getOrElse(0.0) <= 0.4 &&
  Try(r.last.toDouble).getOrElse(0.0) <= 0
)

// Transformation 7: Group students by study load and compute the average stress level for each group
val studyLoadRDD = rows.map(arr => (arr(14).toDouble, arr(20).toInt))
val avgStressPerStudyLoad = studyLoadRDD.groupByKey().mapValues(values => values.sum.toDouble / values.size)

// Actions:

// Action 1: Count Students in Each Stress Category 
val categoryCounts = categorized.reduceByKey(_ + _).collect()
// Print the distribution of Students by Stress Level
println( "Stress Level Categories Count:")
categoryCounts.foreach { case (level, count) =>
  println(s" - $level: $count students")
}

// Action 2: Display the first Vulnerable Student (High Stress + Low Support)
val firstVulnerable = vulnerableStudents.first()
// Print the student's data 
println("\n First Vulnerable Student Record (High Stress + Low Support):")
println("Field".padTo(25, ' ') + "Value")
println("-" * 40)
featureNames.zip(firstVulnerable).foreach { case (name, value) =>
  println(name.padTo(25, ' ') + value)
}

// Action 3: Top 5 Academic Performance Levels with Highest Average Stress
val top5Courses = avgStressByPerformance.takeOrdered(5)(Ordering[Double].reverse.on { case (score, _) => score.toDouble })
// Print the top 5 
println("\nTop 5 Academic Performance Levels with Highest Average Stress:")
top5Courses.zipWithIndex.foreach { case ((scoreStr, avg), i) =>
  val score = scoreStr.toDouble
  val label = score match {
    case s if s < 0.2  => "Very Poor"
    case s if s < 0.4  => "Poor"
    case s if s < 0.6  => "Average"
    case s if s < 0.8  => "Good"
    case _             => "Excellent"
  }
  val roundedScore = BigDecimal(score).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  println(s" ${i + 1}. Performance Level: $label ($roundedScore) → Avg Stress: ${avg.formatted("%.2f")}")
}

// Action 4: Count students per lifestyle cluster
val lifestyleCounts = lifestyleClusters.reduceByKey(_ + _).collect()
// Print the most Frequent Lifestyle Clusters
println("\nTop 5 Most Common Lifestyle Clusters:")
lifestyleCounts.sortBy(-_._2).take(5).foreach { case (cluster, count) =>
  println(s" - $cluster: $count students")
}

// Action 5: Ratio Range Histogram 
val ratioBuckets = ratios.map { case (_, ratio) =>
  val bucket = ratio match {
    case r if r < 0.5  => "[0.0 - 0.5)"
    case r if r < 1.0  => "[0.5 - 1.0)"
    case r if r < 1.5  => "[1.0 - 1.5)"
    case r if r < 2.0  => "[1.5 - 2.0)"
    case _             => "2.0+"
  }
  (bucket, 1)
}.reduceByKey(_ + _)  
// Prints the Sleep-to-Study Ratio 
println("\n Sleep-to-Study Ratio Distribution (Printed via foreach):")
ratioBuckets.foreach { case (range, count) =>
  println(f"$range%-12s → $count students")
}

// Action 6: Collect Sample Students with Balanced Life 
val balancedStudents = balancedCandidates.takeSample(false, 5)
val featureNames = header.split(",") 
// Print sample of Students with a Balanced Lifestyle
println("\nSample of Students with Balanced Lifestyle:")
println(featureNames.mkString(", "))
balancedStudents.foreach(row => println(row.mkString(", ")))

// Action 7: 
val topStressStudyLoads = avgStressPerStudyLoad.takeOrdered(5)(Ordering[Double].reverse.on(_._2))
println("\n Top 5 Study Loads with Highest Avg Stress:")
topStressStudyLoads.foreach { case (studyLoad, avgStress) =>
  println(f"Study Load: $studyLoad%.2f → Avg Stress: ${avgStress.formatted("%.2f")}")
}