# dataflow
Demo data pipeline for TERENO and other station data

| ![dataflow](assets/dataflow.png) | 
|:--:| 
| *General schema for the dataflow* |

## How to work with the prefect.io / docker setup

1. Build prefect flow base image, tag it and push to docker.io
2. Run flow.py script and register with the prefect server on kead1
3. Trigger execution or set a schedule

### Building the base image

Currently the docker.io docker registry is used. To create the base image for the prefect flow build and tag the image. To allow for a consistent versioning follow the *SemVer* versioning scheme.

```
docker build -t cwerner/dataflow:latest .
docker push cwerner/dataflow:latest

# semver versioning (major.minor)
docker tag cwerner/dataflow:latest cwerner/dataflow:0.1
docker push cwerner/dataflow:0.1

# semver versioning (major.minor.patch)
docker tag cwerner/dataflow:latest cwerner/dataflow 0.1.0
docker push cwerner/dataflow:0.1
```

Or use the publish.sh script to achieve the same thing like so:
```
./publish.sh 0.1.0
```
