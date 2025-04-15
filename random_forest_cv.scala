import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

// Create SparkSession
// val spark = SparkSession.builder()
//   .appName("StressLevelPrediction")
//   .master("local[*]")
//   .getOrCreate()
val spark = SparkSession.builder.appName("Student-Stress-Analysis").master("local[*]").getOrCreate()

// Read data
val data_path = "cleaned_student_stress_factors.csv"
// val df = spark.read.format("csv").option("header", "true").load(data_path)

val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_path)

val labeledData = data.withColumnRenamed("stress_level", "label")

val featureCols = Array("anxiety_level", "self_esteem", "mental_health_history",
  "depression", "headache", "blood_pressure", "sleep_quality",
  "breathing_problem", "noise_level", "living_conditions", "safety",
  "basic_needs", "academic_performance", "study_load", 
  "teacher_student_relationship", "future_career_concerns",
  "social_support", "peer_pressure", "extracurricular_activities", "bullying")

val assembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

val rf = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")

val pipeline = new Pipeline().setStages(Array(assembler, rf))

val paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(10, 20, 30))
  .addGrid(rf.maxDepth, Array(3, 5, 7))
  .build()

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid.build())  // Use the built paramGrid
  .setNumFolds(5)

val Array(trainingData, testData) = labeledData.randomSplit(Array(0.7, 0.3))

val cvModel = cv.fit(trainingData)

val predictions = cvModel.transform(testData)
predictions.select("features", "label", "prediction").show(5)

val accuracy = evaluator.evaluate(predictions)
println(s"Test Accuracy = $accuracy")
println(s"Test Error = ${1.0 - accuracy}")

val bestModel = cvModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
val rfModel = bestModel.stages(1).asInstanceOf[RandomForestClassificationModel]
println(s"Best Random Forest Model:\n ${rfModel.toDebugString}")