"""/opt/spark/bin/spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.kafka:kafka-clients:3.4.1   --jars /home/sara/Stream_Kafka/postgresql-42.6.0.jar   --driver-class-path /home/sara/Stream_Kafka/postgresql-42.6.0.jar   /home/sara/Desktop/Covid19/StreamCovid.py"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
from pyspark.sql.functions import from_json, col, to_date, udf, lit, sum as spark_sum, concat_ws, collect_list
from pyspark.ml import PipelineModel
import logging
import re
from pyspark.sql.functions import col, lower, trim, count, date_trunc
from pyspark.sql.functions import when, col, lit
from pyspark.sql.functions import udf, explode, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from rapidfuzz import process, fuzz


# ----------------------------
# 1. Logging and Spark Session
# ----------------------------
logging.basicConfig(
    filename="/home/sara/Desktop/Covid19/Stream_Predict.log",
    level=logging.INFO,
    format='%(asctime)s :: %(levelname)s :: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark = SparkSession.builder.appName("StreamPredict").getOrCreate()
logger.info("SparkSession built successfully.")

# ----------------------------
# 2. Read Data from Kafka
# ----------------------------
rawData = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "CovidF") \
    .option("startingOffsets", "earliest") \
    .load()
logger.info("Read data from Kafka topic successfully.")

json_schema = StructType([
    StructField("event_timestamp", StringType(), True),
    StructField("comment", StringType(), True)
])

parsed_stream = rawData.selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), json_schema).alias("data"))

parsed_stream = parsed_stream.select("data.event_timestamp", "data.comment")

parsed_stream = parsed_stream.withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

# ----------------------------
# 3. Clean and Filter Data
# ----------------------------
def clean_comment(comment):
    if comment is None:
        return None
    comment = comment.lower()
    comment = re.sub(r'\brt\b', '', comment)
    comment = re.sub(r'http\S+|www\S+', '', comment)
    comment = re.sub(r'@\w+', '', comment)
    comment = re.sub(r'#\w+', '', comment)
    comment = re.sub(r'[^a-zA-Z0-9\s]', '', comment)
    comment = re.sub(r'\s+', ' ', comment).strip()
    return comment

clean_comment_udf = udf(clean_comment, StringType())

parsed_stream = parsed_stream.withColumn("clean_text", clean_comment_udf(col("comment")))
parsed_stream = parsed_stream.filter((col("clean_text").isNotNull()) & (col("clean_text") != ""))

# # ----------------------------
# # 4. Define COVID-related Keywords and Filter Data
# # ----------------------------
covid_keywords = {"covid", "covid19", "coronavirus", "pandemic", "lockdown", "quarantine", "mask", "wuhan", "vaccine", "vaccination", "socialdistancing", "stayhome"}

def contains_covid(text):
    if text is None:
        return False
    return any(keyword in text.lower() for keyword in covid_keywords)

contains_covid_udf = udf(contains_covid, BooleanType())

filtered_df = parsed_stream.filter(contains_covid_udf(col("clean_text")))


from pyspark.sql.functions import count
from pyspark.sql.functions import to_date
filtered_df = filtered_df.withColumn("event_timestamp", to_date("event_timestamp"))



# #------------------------------------------------
keyword_dict = {
    "DISEASE": [
        "covid-19", "coronavirus", "influenza", "cardiovascular disease", "diabetes",
        "covid", "asthma", "cancer", "pneumonia", "tuberculosis", "malaria",
    ],
    "DRUG": [
        "remdesivir", "aspirin", "ibuprofen", "acetaminophen", "paracetamol", "dexamethasone",
        "chloroquine", "hydroxychloroquine", "azithromycin", "favipiravir", "lopinavir",
        "ritonavir", "oseltamivir", "molnupiravir", "interferon", "tocilizumab",
        "naproxen", "metformin", "atorvastatin", "amoxicillin"
    ],
    "VIRUS": [
        "sars-cov-2", "mers", "h1n1", "influenza a", "influenza b", "hiv", "zika virus",
        "ebola virus", "hepatitis b", "hepatitis c", "coronavirus", "adenovirus", "rhinovirus"
    ],
    "VACCINE": [
        "pfizer", "moderna", "astrazeneca", "janssen", "sinovac", "sinopharm", "covaxin",
        "covovax", "sputnik v", "novavax", "bnt162b2", "mrna-1273", "bbibp-corv", "covidshield",
        "gardasil", "flu vaccine", "hepatitis b vaccine", "hpv vaccine", "measles vaccine"
    ]
}
from rapidfuzz import process, fuzz
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from rapidfuzz import process, fuzz

# 1. Define the Python function
def fuzzy_extract_best_keywords(text, keyword_dict_broadcast, threshold=95):
    found_entities = []
    lower_text = text.lower()
    for entity_type, keywords in keyword_dict_broadcast.value.items():
        match, score, keyword = process.extractOne(
            lower_text, [kw.lower() for kw in keywords], scorer=fuzz.partial_ratio
        )
        if score >= threshold:
            original_kw = next((kw for kw in keywords if kw.lower() == keyword), keyword)
            found_entities.append((entity_type, original_kw))
    return found_entities

keyword_dict_broadcast = spark.sparkContext.broadcast(keyword_dict)

def udf_wrapper(text):
    return fuzzy_extract_best_keywords(text, keyword_dict_broadcast)

entity_schema = ArrayType(
    StructType([
        StructField("entity_type", StringType(), True),
        StructField("entity_value", StringType(), True)
    ])
)

fuzzy_ner_udf = udf(udf_wrapper, entity_schema)

ner_df = filtered_df.withColumn("keyword_matches", fuzzy_ner_udf(col("clean_text")))

ner_exploded = ner_df.withColumn("match", explode(col("keyword_matches")))

ner_result = ner_exploded \
    .withColumn("entity_type", col("match.entity_type")) \
    .withColumn("entity_value", col("match.entity_value")) \
    .drop("match")

# 7. Final select
final_stream_df = ner_result.select("event_timestamp", "clean_text", "entity_type", "entity_value")

# # ----------------------------
# # 5. Load the Sentiment Model and Make Predictions
# # ----------------------------
custom_model_directory = "/home/sara/Desktop/Covid19/covidfinal/spark_models"
pipelinePath = f"file://{custom_model_directory}/LogisticRegression"
loaded_model = PipelineModel.load(pipelinePath)
logger.info("Model loaded successfully.")
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def map_prediction_to_label(probability_vector):
    prob_positive = float(probability_vector[1])  # Get the probability of the positive class
    if prob_positive > 0.6:
        return "positive"
    elif prob_positive < 0.4:
        return "negative"
    else:
        return "neutral"

map_prediction_udf = udf(map_prediction_to_label, StringType())

predictions = loaded_model.transform(final_stream_df)

result = predictions.withColumn("label", map_prediction_udf(col("probability"))) \
                    .withColumn("data_type", lit("test").cast("string")) \
                    .withColumnRenamed("clean_text", "comment") \
                    .select(
                        "event_timestamp",
                        "comment",
                        "label",
                        "data_type",
                        "entity_type",
                        "entity_value"
                    )



# ----------------------------
# 6. Write to PostgreSQL using foreachBatch
# ----------------------------
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "pass",
    "driver": "org.postgresql.Driver"
}

def process_batch(df, batch_id):
    try:
        # Drop duplicates based on relevant columns before writing
        df = df.dropDuplicates(["event_timestamp", "comment", "entity_type", "entity_value"])

        df.write.jdbc(url=jdbc_url,
                      table="covid_commentsF",
                      mode="append",
                      properties=connection_properties)
        logger.info(f"Batch {batch_id}: Data written to PostgreSQL successfully.")
    except Exception as e:
        logger.error(f"Batch {batch_id}: Error writing to PostgreSQL: {e}")
query = result.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/home/sara/Desktop/Covid19/checkpointCovidF11") \
    .outputMode("append") \
    .start()

query.awaitTermination()
