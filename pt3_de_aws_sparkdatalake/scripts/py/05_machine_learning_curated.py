import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1700191114522 = glueContext.create_dynamic_frame.from_catalog(
    database="lakehouse",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1700191114522",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1700191128342 = glueContext.create_dynamic_frame.from_catalog(
    database="lakehouse",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1700191128342",
)

# Convert DynamicFrames to DataFrames
df1 = AWSGlueDataCatalog_node1700191114522.toDF()
df2 = AWSGlueDataCatalog_node1700191128342.toDF()

# Register DataFrames as temporary views
df1.createOrReplaceTempView("step_trainer_view")
df2.createOrReplaceTempView("accelerometer_view")

# Perform SQL Join
result_df = spark.sql("""
    SELECT *
    FROM step_trainer_view
    JOIN accelerometer_view
    ON step_trainer_view.sensorreadingtime = accelerometer_view.timestamp
""")

# Convert the result DataFrame back to a DynamicFrame
result_dynamic_frame = DynamicFrame.fromDF(result_df, glueContext, "result_dynamic_frame")

# Script generated for node Amazon S3
AmazonS3_node1700191218830 = glueContext.write_dynamic_frame.from_options(
    frame=result_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://ggnp-udacity-bucket-2/machine_learning/curated/",
        "partitionKeys": [],  # You need to provide the correct partition keys if applicable
    },
    transformation_ctx="AmazonS3_node1700191218830",
)

job.commit()