import org.apache.spark.sql.SparkSession


// spark session
val spark = SparkSession.builder.appName("Student-Stress-Analysis").master("local[*]").getOrCreate()
val sc = spark.sparkContext

// read csv file
val data_path = "cleaned_student_stress_factors.csv"
val df = spark.read.format("csv").option("header", "true").load(data_path)


df.createOrReplaceTempView("stressDF")

val query_1 =
"""
SELECT 
    CASE
        WHEN extracurricular_activities <= 0.2 THEN 'Very Low Activity'
        WHEN extracurricular_activities <= 0.4 THEN 'Low Activity'
        WHEN extracurricular_activities <= 0.6 THEN 'Moderate Activity'
        WHEN extracurricular_activities <= 0.8 THEN 'High Activity'
        WHEN extracurricular_activities <= 1 THEN 'Very High Activity'
    END AS Activity_level,
    round(avg(study_load),2) AS AVG_Study_Load, 
    round(avg(stress_level), 2) AS AVG_Stress,
    round(avg(academic_performance), 2) AS AVG_Academic_Performance
FROM stressDF
GROUP BY Activity_level
ORDER BY AVG_Stress
"""
val result_1 = spark.sql(query_1)



val query_2 =
""" 
SELECT 
    CASE
        WHEN academic_performance >= 0.0 AND academic_performance < 0.2 THEN 'Failing'
        WHEN academic_performance >= 0.2 AND academic_performance < 0.4 THEN 'Low'
        WHEN academic_performance >= 0.4 AND academic_performance < 0.6 THEN 'Moderate'
        WHEN academic_performance >= 0.6 AND academic_performance < 0.8 THEN 'Good'
        WHEN academic_performance >= 0.8 AND academic_performance < 1.0 THEN 'High'
        WHEN academic_performance = 1.0 THEN 'Excellent'
    END AS Performance,
    count(academic_performance) AS Nnumber_of_Students,
    round(avg(future_career_concerns), 2) AS AVG_Future_Career_Concerns, 
    round(avg(stress_level), 2) AS AVG_Stress
FROM stressDF
GROUP BY Performance
ORDER BY AVG_Stress
"""
val result_2 = spark.sql(query_2)


val query_3 =
""" 
SELECT 
    CASE
        WHEN academic_performance >= 0.0 AND academic_performance < 0.2 THEN 'Failing'
        WHEN academic_performance >= 0.2 AND academic_performance < 0.4 THEN 'Low'
        WHEN academic_performance >= 0.4 AND academic_performance < 0.6 THEN 'Moderate'
        WHEN academic_performance >= 0.6 AND academic_performance < 0.8 THEN 'Good'
        WHEN academic_performance >= 0.8 AND academic_performance < 1.0 THEN 'High'
        WHEN academic_performance = 1.0 THEN 'Excellent'
    END AS Performance,
    count(academic_performance) AS Nnumber_of_Students,
    round(avg(study_load),2) AS AVG_Study_Load, 
    round(avg(teacher_student_relationship), 2) As AVG_Teacher_Student_Relationship,
    round(avg(stress_level), 2) AS AVG_Stress
FROM stressDF
GROUP BY Performance
ORDER BY AVG_Teacher_Student_Relationship
"""
val result_3 = spark.sql(query_3)


// Query1:
println("How are the activity levels affecting the student performance?")
result_1.show()

// Query2:
println("How are future career concerns affecting Stress Levels for students?")
result_2.show()

// Query3:
println("Is Academic Performance related with study load and teacher_student_relationship ?")
result_3.show()

