import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Initialize job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# ------------------------------------------------------------------------
# ✅ Script generated for node Accelerometer Trusted
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sid-stedi-lakehouse/accelerometer/trusted/"], 
        "recurse": True
    },
    transformation_ctx="AccelerometerTrusted_node"
)

# ✅ Script generated for node Customer Trusted
CustomerTrusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sid-stedi-lakehouse/customer/trusted/"],
        "recurse": True
    },
    transformation_ctx="CustomerTrusted_node"
)

# ------------------------------------------------------------------------
# ✅ Join on email=user between trusted datasets
Join_node = Join.apply(
    frame1=CustomerTrusted_node,
    frame2=AccelerometerTrusted_node,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node"
)

# ------------------------------------------------------------------------
# ✅ Select only customer columns (no accelerometer data)
SqlQuery = '''
SELECT DISTINCT c.*
FROM myDataSource c
'''
SQLQuery_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={"myDataSource": Join_node},
    transformation_ctx="SQLQuery_node"
)

# ------------------------------------------------------------------------
# ✅ Evaluate Data Quality
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# ------------------------------------------------------------------------
# ✅ Write output to curated zone and Glue catalog
CustomerCurated_node = glueContext.getSink(
    path="s3://sid-stedi-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node"
)
CustomerCurated_node.setCatalogInfo(
    catalogDatabase="stedi_2",
    catalogTableName="customer_curated"
)
CustomerCurated_node.setFormat("json")
CustomerCurated_node.writeFrame(SQLQuery_node)

job.commit()
