from pyspark.sql import SparkSession
import numpy as np
import time
from datetime import datetime
from pyspark.sql.functions import greatest, least, col, lag, log
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("BDT Project") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.avro.compression.codec", "snappy") \
    .config("spark.jars",
            "file:///usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,"
            "file:///usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.0.3") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

assets = spark.read.format("avro").table('projectdb.assets')
assets.createOrReplaceTempView('assets')

train = spark.read.format("avro").table('projectdb.train')
train.createOrReplaceTempView('train')

totimestamp = lambda s: np.int32(time.mktime(datetime.strptime(s, "%d/%m/%Y").timetuple()))

train_window = [totimestamp("01/05/2021").item(), totimestamp("30/05/2021").item()]
test_window = [totimestamp("01/06/2021").item(), totimestamp("30/06/2021").item()]

df_btc = train.filter(train.asset_id == '1')

df_btc = df_btc.withColumn("upper_shadow", col("high") - greatest(col("close"), col("open")))
df_btc = df_btc.withColumn("lower_shadow", least(col("close"), col("open")) - col("low"))


def log_return(column, periods=1):
    return log(column) - lag(log(column), periods).over(Window.orderBy("timestamp"))


log_return_col = log_return("vwap")

df_btc = df_btc.withColumn("log_return_1", log_return_col)

features = ['upper_shadow', 'lower_shadow', 'log_return_1']
label = 'target'

df_btc_nnl = df_btc.filter(df_btc.id != '3')

assembler = VectorAssembler(inputCols=features, outputCol="features")

scaler = StandardScaler(inputCol='features', outputCol="scaledFeatures")

pipeline = Pipeline(stages=[assembler, scaler])

scaled_df = pipeline.fit(df_btc_nnl).transform(df_btc_nnl)

train_set = scaled_df.filter(
    (scaled_df.timestamp >= train_window[0]) & (scaled_df.timestamp <= train_window[1])).select('scaledFeatures',
                                                                                                'target')
test_set = scaled_df.filter((scaled_df.timestamp >= test_window[0]) & (scaled_df.timestamp <= test_window[1])).select(
    'scaledFeatures', 'target')

train_set_dbl = train_set.withColumn("target", col("target").cast("double"))
test_set_dbl = test_set.withColumn("target", col("target").cast("double"))

lr = LinearRegression(featuresCol='scaledFeatures', labelCol='target')
pipeline = Pipeline(stages=[lr])
model = pipeline.fit(train_set_dbl)
predictions = model.transform(test_set_dbl)

evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
print("R-squared on test data = %g" % r2)

model.save("/BD_project/models/LR")

predictions_string = predictions.limit(200).toPandas().to_csv(index=False)

with open("/BD_project/output/LRpredictions.csv", 'w') as f:
    f.write(predictions_string)
