FROM python:3.8

# ARG DEBIAN_FRONTEND=noninteractive
#
# RUN apt-get update -y && \
#     apt-get install -y --no-install-recommends sshpass

COPY requirements.txt /home/requirements.txt
RUN pip install --use-feature=2020-resolver -r /home/requirements.txt

COPY ./great_expectations /home/great_expectations
COPY ./src /home/src
COPY setup.py /home/setup.py
RUN cd /home && pip install .

