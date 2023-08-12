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

# Script generated for node Step Trainer IoT
StepTrainerIoT_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mathieu/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerIoT_node1",
)

# Script generated for node customers_curated
customers_curated_node1691853979309 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-mathieu/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="customers_curated_node1691853979309",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1691854138986 = ApplyMapping.apply(
    frame=StepTrainerIoT_node1,
    mappings=[
        ("sensorReadingTime", "long", "right_sensorReadingTime", "long"),
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("distanceFromObject", "int", "right_distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1691854138986",
)

# Script generated for node SQL Query
SqlQuery0 = """
select DISTINCT serialNumber from myDataSource
"""
SQLQuery_node1691854037618 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": customers_curated_node1691853979309},
    transformation_ctx="SQLQuery_node1691854037618",
)

# Script generated for node Join
Join_node1691854088530 = Join.apply(
    frame1=SQLQuery_node1691854037618,
    frame2=RenamedkeysforJoin_node1691854138986,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1691854088530",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1691854088530,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-mathieu/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
