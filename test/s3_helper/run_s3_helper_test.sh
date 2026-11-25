#!/bin/bash
# Tests LINZ::GNSS::AwsS3Bucket (python helper and aws client) in Docker images, against a
# LocalStack S3 endpoint, using the modules in this source tree rather than those installed
# in the image.  No AWS account is used.
#
# Usage: run_s3_helper_test.sh image [clients]
#
#   image    an image with liblinz-gnss-perl and its dependencies installed, such as a
#            linz-bernese54-py or gnss-datum-processing image
#   clients  the S3 clients to test, quoted (default "python aws").  For an image without
#            python3/boto3 use "auto aws" with TEST_AUTO_CLIENT=aws, to check that auto falls
#            back to the aws client.
#
# LOCALSTACK_IMAGE overrides the LocalStack image (default localstack/localstack:4.9).

set -e
image=${1:?Usage: run_s3_helper_test.sh image [clients]}
clients=${2:-python aws}
localstack_image=${LOCALSTACK_IMAGE:-localstack/localstack:4.9}
srcdir=$(cd "$(dirname "$0")/../.." && pwd)
network=s3-helper-test-$$
localstack=s3-helper-localstack-$$

cleanup() {
    docker rm -f "$localstack" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$network" >/dev/null
docker run -d --name "$localstack" --network "$network" -e SERVICES=s3 "$localstack_image" >/dev/null
echo "Waiting for LocalStack"
for i in $(seq 60); do
    if docker exec "$localstack" curl -sf http://localhost:4566/_localstack/health 2>/dev/null | grep -q '"s3": "\(available\|running\)"'; then
        break
    fi
    sleep 1
done

docker run --rm --network "$network" \
    -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=ap-southeast-2 \
    -e AWS_ENDPOINT_URL="http://$localstack:4566" \
    -e PERL5LIB=/src/lib -e TEST_LOG_LEVEL -e TEST_AUTO_CLIENT \
    -v "$srcdir:/src:ro" \
    --entrypoint sh "$image" -c "
        aws s3api create-bucket --bucket s3-helper-test \
            --create-bucket-configuration LocationConstraint=ap-southeast-2 >/dev/null &&
        perl /src/test/s3_helper/s3_helper_test.pl s3-helper-test $clients"
