#!/bin/sh
set -e

DOCKERIO=cwerner

echo "Building and publishing DataFlow base image."

docker build --no-cache -t $DOCKERIO/dataflow:latest .

if [ -z "$1" ]
then 
    echo "No version provided only pushing <latest> tag."
    docker push $DOCKERIO/dataflow:latest
else
    echo "Pushing <latest> tag."
    docker push $DOCKERIO/dataflow:latest

    echo "Semantic version: '$1'"

    version="$1"
    a=( ${version//./ } )
    len=${#a[@]}

    if [[ "$len" == 2 ]]
    then
        # semver versioning (major.minor.0)
        v_maj_min="${a[0]}.${a[1]}"
        v_maj_min_p="${a[0]}.${a[1]}.0"
    elif [[ "$len" == 3 ]]
    then
        # semver versioning (major.minor.patch)
        v_maj_min="${a[0]}.${a[1]}"
        v_maj_min_p="${a[0]}.${a[1]}.${a[2]}"
    else
        echo "Version provided does not follow semver rules."
        exit 1
    fi

    docker tag $DOCKERIO/dataflow:latest cwerner/dataflow:$v_maj_min_p
    docker push $DOCKERIO/dataflow:$v_maj_min_p
    docker tag $DOCKERIO/dataflow:latest cwerner/dataflow:$v_maj_min
    docker push $DOCKERIO/dataflow:$v_maj_min
fi

