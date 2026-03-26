import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3

# -----------------------------
# CONFIG
# -----------------------------
bucket_name = 'project-de-datewithdata-dhairya'
prefix = 'warehouse/'

s3 = boto3.client('s3')

# -----------------------------
# SAFE UNION FUNCTION (NO COUNT)
# -----------------------------
def sparkUnion(glueContext, mapping, transformation_ctx):
    dfs = [frame.toDF() for frame in mapping.values()]
    
    if len(dfs) == 0:
        raise Exception("No dataframes to union")

    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# -----------------------------
# INIT
# -----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# READ DATA
# -----------------------------
print("Reading Mark Data...")
mark_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-de-datewithdata-dhairya/staging/mark/"],
        "recurse": True,
    }
)

print("Reading Student Data...")
student_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-de-datewithdata-dhairya/staging/student/"],
        "recurse": True,
    }
)

print("Reading Existing Warehouse Data...")
dw_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://project-de-datewithdata-dhairya/warehouse/"],
        "recurse": True,
    }
)

# -----------------------------
# TRANSFORM (JOIN)
# -----------------------------
print("Performing Join...")
joined_df = Join.apply(
    frame1=mark_df,
    frame2=student_df,
    keys1=["student_id"],
    keys2=["id"]
)

# -----------------------------
# UNION (MERGE WITH OLD DATA)
# -----------------------------
print("Performing Union...")
final_df = sparkUnion(
    glueContext,
    mapping={
        "new_data": joined_df,
        "old_data": dw_df
    },
    transformation_ctx="final_union"
)

# -----------------------------
# SAFE DELETE (FIXED BUG)
# -----------------------------
print("Cleaning old warehouse data...")
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if 'Contents' in response:
    for obj in response['Contents']:
        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    print("Old files deleted")
else:
    print("No existing files to delete")

# -----------------------------
# WRITE OUTPUT
# -----------------------------
print("Writing final data to S3...")
glueContext.write_dynamic_frame.from_options(
    frame=final_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://project-de-datewithdata-dhairya/warehouse/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"}
)

print("Job Completed Successfully")
job.commit()
