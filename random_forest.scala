import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession


// Load and parse the data file, converting it to a DataFrame.
// val data = spark.read.format("libsvm").load("cleaned_student_stress_factors.csv")
val spark = SparkSession.builder.appName("RandomForestExample").master("[local[*]]").getOrCreate()
val data = spark.read.option("header", "true").option("inferSchema", "true").csv("cleaned_student_stress_factors.csv")

// // Index labels, adding metadata to the label column.
// // Fit on whole dataset to include all labels in index.
// val labelIndexer = new StringIndexer()
//   .setInputCol("stress_level")
//   .setOutputCol("indexedLabel")
//   .fit(data)

val featureCols = data.columns.filter(_ != "stress_level")
val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")


val finalData = data.withColumnRenamed("stress_level", "label")   
// val assembler = new VectorAssembler()
//                 .setInputCol(Array("anxiety_level", "self_esteem", "mental_health_history",
//                                   "depression", "headache", "blood_pressure", "sleep_quality",
//                                   "breathing_problem", "noise_level", "living_conditions", "safety",
//                                    "basic_needs", "academic_performance", "study_load", 
//                                   "teacher_student_relationship", "future_career_concerns",
//                                    "social_support", "peer_pressure", "extracurricular_activities", "bullying"))


// // Automatically identify categorical features, and index them.
// // Set maxCategories so features with > 4 distinct values are treated as continuous.
// val featureIndexer = new VectorIndexer()
//   .setInputCol("features")
//   .setOutputCol("indexedFeatures")
//   .setMaxCategories(4)
//   .fit(data)

// Split the data into training and test sets (30% held out for testing).
val Array(trainingData, testData) = finalData.randomSplit(Array(0.7, 0.3), seed = 1234L)

// Train a RandomForest model.
val rf = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(10)

// // Convert indexed labels back to original labels.
// val labelConverter = new IndexToString()
//   .setInputCol("prediction")
//   .setOutputCol("predictedLabel")
//   .setLabels(labelIndexer.labelsArray(0))

// // Chain indexers and forest in a Pipeline.
// val pipeline = new Pipeline()
//   .setStages(Array(labelIndexer, assembler, featureIndexer, rf, labelConverter))

val pipeline = new Pipeline()
    .setStages(Array(assembler, rf))

// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("features", "label", "prediction").show(5)

// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println(s"Test Error = ${(1.0 - accuracy)}")

val rfModel = model.stages(1).asInstanceOf[RandomForestClassificationModel]
println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
