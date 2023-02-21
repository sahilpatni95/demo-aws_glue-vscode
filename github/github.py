# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job

# ## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# job.commit()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="test-03022023-csv",
    table_name="input",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("player", "string", "player", "string"),
        ("nationality", "string", "nationality", "string"),
        ("type", "string", "type", "string"),
        (" price ", "string", " price ", "string"),
        ("sold/unsold", "string", "sold/unsold", "string"),
        ("team", "string", "team", "string"),
        ("year", "long", "year", "long"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://test-03022023/output/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="test-03022023-csv", catalogTableName="par"
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()