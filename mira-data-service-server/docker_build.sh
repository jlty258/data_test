#!/bin/bash

show_help() {
    echo "Usage: $0 -t <tag> [-p <true|false>] [-r <registry_address>] [-d <dockerfile_path>]"
    echo "Options:"
    echo "  -t <tag>                Specify the tag for Docker images."
    echo "  -p <true|false>         Push the Docker images to the registry. (Default: false)"
    echo "  -r <registry_address>   Specify the address of the Docker registry."
    echo "                          If not provided, Docker Hub is assumed as the default registry."
    echo "  -d <dockerfile_path>    Specify the path to the Dockerfile. (Default: Dockerfile)"
}

# Check if the number of parameters is correct
if [ $# -lt 1 ]; then
    show_help
    exit 1
fi

# Default values
push_flag=false
dockerfile_path="Dockerfile"

# Parse command line options
while getopts ":t:p:r:d:" opt; do
    case ${opt} in
        t)
            docker_tag=$OPTARG
            ;;
        p)
            push_flag=$OPTARG
            if [[ ! $OPTARG =~ ^(true|false)$ ]]; then
                echo "Invalid argument for -p. Please provide 'true' or 'false'." >&2
                show_help
                exit 1
            fi
            ;;
        r)
            registry_address=$OPTARG
            ;;
        d)
            dockerfile_path=$OPTARG
            ;;
        \?)
            echo "Invalid option: $OPTARG" >&2
            show_help
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            show_help
            exit 1
            ;;
    esac
done

shift $((OPTIND -1))

# Check if tag is provided
if [ -z "$docker_tag" ]; then
    echo "Tag not specified."
    show_help
    exit 1
fi

echo "Building mira-data-service environment image using ${dockerfile_path}"

docker build -f ./${dockerfile_path} -t chainweaver/mira-data-service:${docker_tag} .

if [ -n "$registry_address" ]; then
    docker tag chainweaver/mira-data-service:${docker_tag} ${registry_address}/chainweaver/mira-data-service:${docker_tag}
fi

if [ "$push_flag" == "true" ]; then
    if [ -z "$registry_address" ]; then
        docker push chainweaver/mira-data-service:${docker_tag}
    else
        docker push ${registry_address}/chainweaver/mira-data-service:${docker_tag}
    fi
fi