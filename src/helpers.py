import os

s3_kwargs = dict(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint_url="https://s3.imk-ifu.kit.edu:8082",
)
