import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mathieu/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1691854538830 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mathieu/accelerator_trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1691854538830",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1691854632248 = ApplyMapping.apply(
    frame=step_trainer_trusted_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("right_distanceFromObject", "int", "right_right_distanceFromObject", "int"),
        ("right_sensorReadingTime", "long", "right_right_sensorReadingTime", "long"),
        ("right_serialNumber", "string", "right_right_serialNumber", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1691854632248",
)

# Script generated for node Join
Join_node1691854618124 = Join.apply(
    frame1=accelerometer_trusted_node1691854538830,
    frame2=RenamedkeysforJoin_node1691854632248,
    keys1=["serialNumber", "timeStamp"],
    keys2=["right_serialNumber", "right_right_sensorReadingTime"],
    transformation_ctx="Join_node1691854618124",
)

# Script generated for node SQL Query
SqlQuery0 = """
select right_serialNumber AS serialNumber
,right_right_distanceFromObject AS distanceFromObject
,right_right_sensorReadingTime AS sensorReadingTime
,x
,y
,z 
from myDataSource
"""
SQLQuery_node1691854952228 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Join_node1691854618124},
    transformation_ctx="SQLQuery_node1691854952228",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1691854952228,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-mathieu/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
