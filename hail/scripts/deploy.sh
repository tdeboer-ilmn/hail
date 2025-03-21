#!/bin/bash

set -ex

retry() {
    "$@" ||
        (sleep 2 && "$@") ||
        (sleep 5 && "$@");
}

[[ $1 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
[[ $2 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
[[ $3 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
git cat-file -e $3^{commit} || (echo "bad sha $3" ; exit 1)
[[ $4 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
[[ $5 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
[[ $6 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
[[ $7 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
[[ $8 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)
[[ $9 ]] || (echo "./deploy.sh HAIL_PIP_VERSION HAIL_VERSION GIT_VERSION REMOTE WHEEL GITHUB_OAUTH_HEADER_FILE HAIL_GENETICS_HAIL_IMAGE WHEEL_FOR_AZURE WEBSITE_TAR" ; exit 1)

HAIL_PIP_VERSION=$1
HAIL_VERSION=$2
GIT_VERSION=$3
REMOTE=$4
WHEEL=$5
GITHUB_OAUTH_HEADER_FILE=$6
HAIL_GENETICS_HAIL_IMAGE=$7
WHEEL_FOR_AZURE=$8
WEBSITE_TAR=$9

retry skopeo inspect $HAIL_GENETICS_HAIL_IMAGE || (echo "could not pull $HAIL_GENETICS_HAIL_IMAGE" ; exit 1)

if git ls-remote --exit-code --tags $REMOTE $HAIL_PIP_VERSION
then
    echo "tag $HAIL_PIP_VERSION already exists"
    exit 0
fi

if [ ! -f $WHEEL ]
then
    echo "wheel not found at $WHEEL"
    exit 1
fi

pip_versions_file=$(mktemp)
pip install hail== 2>&1 \
    | head -n 1 \
    | sed 's/.*versions: //' \
    | sed 's/)//' \
    | sed 's/ //g' \
    | tr ',' '\n' \
         > $pip_versions_file

if grep -q -e $HAIL_PIP_VERSION $pip_versions_file
then
    echo "package $HAIL_PIP_VERSION already exists"
    exit 1
fi

if curl -sf https://api.github.com/repos/hail-is/hail/releases/tags/$HAIL_PIP_VERSION >/dev/null
then
    echo "release $HAIL_PIP_VERSION already exists"
    exit 1
fi

# push git tag
git tag $HAIL_PIP_VERSION -m "Hail version $HAIL_PIP_VERSION."
git push origin $HAIL_PIP_VERSION

# make GitHub release
curl -XPOST -H @$GITHUB_OAUTH_HEADER_FILE https://api.github.com/repos/hail-is/hail/releases -d '{
  "tag_name": "'$HAIL_PIP_VERSION'",
  "target_commitish": "main",
  "name": "'$HAIL_PIP_VERSION'",
  "body": "Hail version '$HAIL_PIP_VERSION'\n\n[Change log](https://hail.is/docs/0.2/change_log.html#version-'${HAIL_PIP_VERSION//[\.]/-}')",
  "draft": false,
  "prerelease": false
}'

retry skopeo copy $HAIL_GENETICS_HAIL_IMAGE docker://docker.io/hailgenetics/hail:$HAIL_PIP_VERSION
retry skopeo copy $HAIL_GENETICS_HAIL_IMAGE docker://us-docker.pkg.dev/hail-vdc/hail/hailgenetics/hail:$HAIL_PIP_VERSION

# deploy to PyPI
twine upload $WHEEL

# deploy wheel for Azure HDInsight
wheel_for_azure_url=gs://hail-common/azure-hdinsight-wheels/$(basename $WHEEL_FOR_AZURE)
gsutil cp $WHEEL_FOR_AZURE $wheel_for_azure_url
gsutil -m retention temp set $wheel_for_azure_url

# update docs sha
cloud_sha_location=gs://hail-common/builds/0.2/latest-hash/cloudtools-5-spark-2.4.0.txt
printf "$GIT_VERSION" | gsutil cp  - $cloud_sha_location
gsutil acl set public-read $cloud_sha_location

# deploy datasets (annotation db) json
datasets_json_url=gs://hail-common/annotationdb/$HAIL_VERSION/datasets.json
gsutil cp python/hail/experimental/datasets.json $datasets_json_url
gsutil -m retention temp set $datasets_json_url

# Publish website
website_url=gs://hail-common/website/$HAIL_PIP_VERSION/www.tar.gz
gsutil cp $WEBSITE_TAR $website_url
gsutil -m retention temp set $website_url

# Create pull request to update Terra and AoU Hail versions
terra_docker_dir=$(mktemp -d)
update_terra_image_py="$(cd "$(dirname "$0")" && pwd)/update-terra-image.py"
git clone https://github.com/DataBiosphere/terra-docker $terra_docker_dir
pushd $terra_docker_dir
git config user.name hail
git config user.email hail@broadinstitute.org

make_pr_for() {
    branch_name=update-$1-to-hail-$HAIL_PIP_VERSION
    git checkout -B $branch_name
    python3 $update_terra_image_py $HAIL_PIP_VERSION $1
    git commit -m "Update $1 to Hail version $HAIL_PIP_VERSION" -- config/conf.json $1
    git push -f origin HEAD
    echo "{
  \"head\": \"$branch_name\",
  \"base\": \"master\",
  \"title\": \"Update $1 to Hail $HAIL_PIP_VERSION\"
}"
    curl -XPOST -H @$GITHUB_OAUTH_HEADER_FILE https://api.github.com/repos/DataBiosphere/terra-docker/pulls -d "{
  \"head\": \"$branch_name\",
  \"base\": \"master\",
  \"title\": \"Update $1 to Hail $HAIL_PIP_VERSION\"
}"
    git reset --hard HEAD
    git checkout master
}

make_pr_for terra-jupyter-hail
make_pr_for terra-jupyter-aou
