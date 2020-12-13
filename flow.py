import prefect
from prefect import Flow, task
from prefect.core.parameter import Parameter
from prefect.environments.storage import Docker
from prefect.artifacts import create_markdown

from prefect.engine.result.base import Result

#from prefect.tasks.secrets import PrefectSecret, EnvVarSecret
from prefect.run_configs import UniversalRun
from prefect.engine.results import PrefectResult
from prefect.engine.serializers import PandasSerializer

from prefect.tasks.great_expectations.checkpoints import RunGreatExpectationsValidation

import pandas as pd

from pathlib import Path
import os

import pendulum
import great_expectations as ge
from src.general import *




# Define checkpoint task
validation_task = RunGreatExpectationsValidation()

# Task for retrieving batch kwargs including csv dataset
@task(log_stdout=True, result=Result(serializer=PandasSerializer('csv')))
def get_batch_kwargs(datasource_name, dataset):
    logger = prefect.context.get("logger")
    dataset = ge.read_csv(Path('/home/data') / dataset)
    logger.info(f"CURRENT WD: {[str(x) for x in Path('/home/data').iterdir()]}")

    logger.info(f'XXX { {"dataset": dataset, "datasource": datasource_name} }')

    return {"dataset": dataset, "datasource": datasource_name}


@task
def report(filename, df=None):
    md_content = f"# DataFlow status\n Looking for file: **{filename}**\n"

    if df is not None:
        sample_table = df.head().iloc[:, 0:8].to_markdown(index=False)
        md_content += sample_table

    create_markdown(md_content)

    return None


def create_filename(sitename, date):
    assert 2010 < date.year, "jday must be in range >= 2010"
    
    sitename_short = sitename.capitalize()[:3]
    year_short = str(date.year)[2:4]
    jday = date.timetuple().tm_yday

    return f"{sitename_short}_M_{year_short}_{jday}.dat"


@task(log_stdout=True, result=Result(serializer=PandasSerializer('csv')))
def pull_rawfile(datalocation, sitename, current_time, offset):
    logger = prefect.context.get("logger")

    current_time = current_time or pendulum.now("utc") # uses "now" if not provided
    if isinstance(current_time, str):
        current_time = pendulum.parse(current_time)

    logger.info(f"time 1: {current_time}")

    previous_time = current_time.subtract(days=offset)

    logger.info(f"time 2: {previous_time}")


    sitename_short = sitename_short = sitename.capitalize()[:3]
    filename = create_filename(sitename, previous_time)

    logger.info(f"Looking for file '{filename}' on server")

    basepath = Path(f"{datalocation}/{sitename}/micromet/raw/slow_response")
    target = basepath / str(previous_time.year) / filename
    header = basepath / f"{sitename_short}_M_header.csv"

    header = pd.read_csv(header).columns.values

    df = pd.read_csv(target, names=header, parse_dates=True)
    logger.info(f" {df.head().iloc[:, 0:8]}")

    outname = target.name.replace(".dat", ".csv")
    outpath = Path("/home") / "data"
    outpath.mkdir(exist_ok=True)

    outfile = outpath / target.name.replace(".dat", ".csv")
    df.to_csv(outfile, index=False)

    return str(outfile.name)


# NOTE: Use PrefectResult for debugging (this will be
#       available to prefect server ui), but switch to
#       S3Result for larger data and better security
#       later

with Flow("TERENO Test Flow",
    result=PrefectResult(),
    storage = Docker(registry_url="cwerner", 
                     image_name="dataflow",
                     base_image="cwerner/dataflow:latest",
                     )) as flow:


    # parameter
    current_time = Parameter("current_time", default=None)
    offset = Parameter('offset', default = 10)  # local 200
    sitename = Parameter('sitename', default = 'fendt')
    datalocation = Parameter('datalocation', default='/rawdata')    # local data

    expectation_suite_name = Parameter("expectation_suite_name", default="Fendt_test.warning")

    targetfile = pull_rawfile(datalocation, sitename, current_time, offset)

    batch_kwargs = get_batch_kwargs("data__dir", targetfile)

    validation_task(
            batch_kwargs=batch_kwargs,
            expectation_suite_name=expectation_suite_name,
            context_root_dir="/home/great_expectations"
        )

    #report(sitename, df)


if __name__ == "__main__":
    #flow.run(run_on_schedule=False)
    #built_storage = flow.storage.build(push=False)
    #print(built_storage.flows)
    #print(built_storage.flow)
    flow.run_config = UniversalRun(labels=["dev"])
    flow.register(project_name="DataFlow")
