import gzip
import io
import mimetypes

from pathlib import Path
from prefect import Task
from prefect.utilities.aws import get_boto_client
from prefect.utilities.tasks import defaults_from_attrs

from typing import Union

class S3UploadDir(Task):
    """
    Task for uploading directory to S3 bucket.
    Note that all initialization arguments can optionally be provided or overwritten at runtime.
    For authentication, there are two options: you can set a Prefect Secret containing your AWS
    access keys which will be passed directly to the `boto3` client, or you can [configure your
    flow's runtime
    environment](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#guide-configuration)
    for `boto3`.
    Args:
        - bucket (str, optional): the name of the S3 Bucket to upload to
        - boto_kwargs (dict, optional): additional keyword arguments to forward to the boto client.
        - **kwargs (dict, optional): additional keyword arguments to pass to the
            Task constructor
    """
    def __init__(self, bucket: str = None, boto_kwargs: dict = None, **kwargs):
        self.bucket = bucket
        if boto_kwargs is None:
            self.boto_kwargs = {}
        else:
            self.boto_kwargs = boto_kwargs
        super().__init__(**kwargs)

    @defaults_from_attrs("bucket")
    def run(
        self,
        source: Union[Path, str],
        credentials: dict = None,
        bucket: str = None,
        compression: str = None,
        ):
        """
        Task run method.
        Args:
            - source (Path, str): the source directory
            - credentials (dict, optional): your AWS credentials passed from an upstream
                Secret task; this Secret must be a JSON string
                with two keys: `ACCESS_KEY` and `SECRET_ACCESS_KEY` which will be
                passed directly to `boto3`.  If not provided here or in context, `boto3`
                will fall back on standard AWS rules for authentication.
            - bucket (str, optional): the name of the S3 Bucket to upload to
            - compression (str, optional): specifies a file format for compression,
                compressing data before upload. Currently supports `'gzip'`.
        Returns:
            - (List(str)): the name of the Keys the data payload was uploaded to
        """

        if bucket is None:
            raise ValueError("A bucket name must be provided.")

        s3_client = get_boto_client("s3", credentials=credentials, **self.boto_kwargs)

        if isinstance(source, str):
            source = Path(source)

        keys = []

        for p in source.rglob("*"):
            if p.is_file():

                relpath = p.relative_to(source)

                data = open(p, 'rb').read()

                # compress data if compression is specified
                if compression:
                    if compression == "gzip":
                        data = gzip.compress(data)
                    else:
                        raise ValueError(f"Unrecognized compression method '{compression}'.")

                # prepare data
                try:
                    stream = io.BytesIO(data)
                except TypeError:
                    stream = io.BytesIO(data.encode())

                # upload
                key = str(relpath) 
                content_type = mimetypes.guess_type(relpath)[0]
                s3_client.upload_fileobj(stream, 
                                         Bucket=bucket, 
                                         Key=key, 
                                         ExtraArgs={'ACL': 'public-read', 'ContentType': content_type})
                keys.append(key)

        return keys

