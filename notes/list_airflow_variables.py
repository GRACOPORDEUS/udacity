from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

print(Variable.get('region'))
print(Variable.get('s3_bucket'))
print(Variable.get('s3_prefix_log_data'))
print(Variable.get('s3_prefix_log_json_path'))
print(Variable.get('s3_prefix_song_data'))



hook = S3Hook(aws_conn_id='aws_credentials')
bucket = Variable.get('s3_bucket')
prefix = Variable.get('s3_prefix')
logging.info(f"Listing Keys from {bucket}/{prefix}")
keys = hook.list_keys(bucket, prefix=prefix)
for key in keys:
    logging.info(f"- s3://{bucket}/{key}")
list_keys()

list_keys_dag = list_keys()