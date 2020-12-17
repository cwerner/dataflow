import html
import io
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import pendulum
import prefect
from prefect import Flow, task
from prefect.core.parameter import Parameter
from prefect.engine.results import S3Result
from prefect.engine.serializers import PickleSerializer
from prefect.engine.state import Success
from prefect.environments.storage import Docker
from prefect.run_configs import UniversalRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.aws.s3 import S3Upload
from prefect.tasks.great_expectations.checkpoints import RunGreatExpectationsValidation
from prefect.tasks.notifications.email_task import EmailTask
from prefect.triggers import any_failed, all_finished

from src.helpers import s3_kwargs
from src.tasks import S3UploadDir


# TODO: match this with GE parsing
def parse_dat_file(target: Union[Path, str], header: Union[Path, str]) -> pd.DataFrame:
    """Parse .dat data file using header file for colnames"""
    colnames = pd.read_csv(header).columns.values
    return pd.read_csv(target, names=colnames, parse_dates=True)


upload_to_s3 = S3Upload(boto_kwargs=s3_kwargs)
upload_dir_to_s3 = S3UploadDir(boto_kwargs=s3_kwargs, trigger=all_finished)

@task(trigger=any_failed)
def email_on_failure(notification_email):
    """Send email on FAIL state"""

    flow_name = prefect.context.flow_name

    # TODO: Check how to create a custom prefect email
    task = EmailTask(
        subject=f"Prefect alert: {flow_name}",
        msg=html.escape(
            f"{flow_name} GreatExpectation Validation failed."
            ),
        email_from="christian.werner@kit.edu",
        email_to=notification_email,
        smtp_server="smtp.kit.edu",
        smtp_port=25,
        smtp_type="STARTTLS",
    ).run()
    return task


def flip_fail_to_success(task, old_state, new_state):
    """A cheaky state_handler that flips a fail outcome to success"""
    if new_state.is_failed():
        return_state = Success(result=new_state.result)
    else:
        return_state = new_state
    return return_state


def create_filename(sitename: str, date: str) -> str:
    """Create filename from site and date"""
    assert 2010 < date.year, "jday must be in range >= 2010"

    sitename_short = sitename.capitalize()[:3]
    year_short = str(date.year)[2:4]
    jday = date.timetuple().tm_yday
    return f"{sitename_short}_M_{year_short}_{jday}.dat"

@task
def create_flags(validation):
    """Use validation results and create a flag dataframe"""
    df_flags = pd.DataFrame()

    return df_flags


# Define checkpoint task
validation_task = RunGreatExpectationsValidation(
#    state_handlers=[flip_fail_to_success]
)


@task
def get_batch_kwargs(datasource_name, dataset):
    """Retrieve batch kwargs including csv dataset"""
    dataset = pd.read_csv(Path("/home/data") / dataset)
    return {"dataset": dataset, "datasource": datasource_name}


@task
def retrieve_and_parse_target_file(
    location: str, site: str, current_time: Optional[str], offset: int = 0
):
    """Retrieve and parse target file"""
    current_time = current_time or pendulum.now("utc")
    if isinstance(current_time, str):
        current_time = pendulum.parse(current_time)

    target_date = current_time.subtract(days=offset)

    filename = create_filename(site, target_date)

    basepath = Path(location) / site / "micromet" / "raw" / "slow_response"
    target = basepath / str(target_date.year) / filename

    site_short = site.capitalize()[:3]
    header = basepath / f"{site_short}_M_header.csv"

    outpath = Path("/home") / "data"
    outpath.mkdir(exist_ok=True)

    outfile = outpath / target.name.replace(".dat", ".csv")

    df = parse_dat_file(target, header)
    df.to_csv(outfile, index=False)

    return str(outfile.name)


# @task(log_stdout=True, trigger=all_finished)
# def upload_folder_to_s3(source: Union[Path, str], bucket: str, prefix: str=""):
#     """Upload content of local directory to a s3 bucket"""

#     # "great_expectations/uncommitted/data_docs/local_site"
    
#     logger = prefect.context.get("logger")

#     if isinstance(source, str):
#         source = Path(source)
#     source = source / "uncommitted" / "data_docs" / "local_site"

#     for p in sorted(source.rglob("*")):
#         if p.is_file():
#             logger.info(f"Directory: {source}")

#             relpath = p.parent
#             filename = p.name

#             # Invoke upload function
#             task = S3Upload(bucket=bucket, boto_kwargs=s3_kwargs).run(open(p, 'rb').read(), key=str(relpath / filename))
#     return None

@task
def prepare_df_for_s3(df: pd.DataFrame) -> str:
    """Convert dataframe for s3 upload"""
    csv_str = io.StringIO()
    df.to_csv(csv_str)
    return csv_str.getvalue()


result = S3Result(
    bucket="dataflow-ge-dailydata",
    location="{task_name}.pickle",
    boto3_kwargs=s3_kwargs,
    serializer=PickleSerializer(),
)


storage = Docker(
    registry_url="cwerner",
    image_name="dataflow",
    base_image="cwerner/dataflow:latest",
    env_vars={
        "PREFECT__LOGGING__LEVEL": "INFO",
        "PREFECT__LOGGING__EXTRA_LOGGERS": "['great_expectations']",
    },
)


with Flow(
    "TERENO Test Flow",
    result=result,
    run_config=UniversalRun(labels=["dev"]),
    schedule=Schedule(clocks=[CronClock("0 6 * * *")]),
    storage=storage,
) as flow:

    # parameters
    current_time = Parameter("current_time", default=None)
    offset = Parameter("offset", default=10)
    sitename = Parameter("sitename", default="fendt")
    datalocation = Parameter("datalocation", default="/rawdata")
    expectation_suite_name = Parameter("expectation_suite_name", default="fendt.demo")
    notification_email = Parameter(
        "notification_email", default="freizeitbeauftragter@gmail.com"
    )

    targetfile = retrieve_and_parse_target_file(
        datalocation, sitename, current_time, offset
    )

    batch_kwargs = get_batch_kwargs("data__dir", targetfile)

    # validate based on ge expectations
    validation = validation_task(
        batch_kwargs=batch_kwargs,
        expectation_suite_name=expectation_suite_name,
        context_root_dir="/home/great_expectations",
    )
    state = email_on_failure(notification_email, upstream_tasks=[validation])

    uploaded = upload_dir_to_s3("/home/great_expectations/uncommitted/data_docs/local_site", 
                                bucket="dataflow-ge-docs",
                                upstream_tasks=[validation]) 

    df_flags = create_flags(validation)

    # upload level 1 data to s3
    data_str = prepare_df_for_s3(batch_kwargs["dataset"])
    uploaded = upload_to_s3(data_str, targetfile, bucket="dataflow-lvl1")

    # upload level 1 flags to s3
    #flags_str = prepare_df_for_s3(df_flags)
    #uploaded = upload_to_s3(data_str, targetfile, bucket="dataflow-lvl1")

if __name__ == "__main__":
    # flow.run(run_on_schedule=False)
    # built_storage = flow.storage.build(push=False)
    flow.register(project_name="DataFlow")
