#!/bin/bash

# Uploads the files to the Syapse python packages repo on S3.
# By default, uploads current version to internal directory.

export AWS_DEFAULT_REGION=us-west-1

if [ "$1" == "prod" ]; then
    PACKAGE_PATH=python_packages
elif [ "$1" == "" -o "$1" == "dev" ]; then
    PACKAGE_PATH=dev_python_packages
elif [ "$1" == "" -o "$1" == "internal" ]; then
    PACKAGE_PATH=internal_python_packages
elif [ "$1" == "" -o "$1" == "external" ]; then
    PACKAGE_PATH=external_python_packages
else
    echo "Argument must be prod, dev, internal, or external."
    exit 1
fi

if ! (pip list | grep awscli > /dev/null); then
    pip install awscli
fi

VERSION=2.2.2
PACKAGE_NAME=pynesis

if [ "$2" == "" ]; then
    TARNAME="$PACKAGE_NAME-$VERSION.tar.gz"
    PACKAGE="$PACKAGE_NAME-$VERSION.tar.gz"
    if [ ! -e dist/$TARNAME ] && [ -e dist/$PACKAGE ]; then
        TARNAME="$PACKAG_NAME-$VERSION.tar.gz"
    fi
    if [ ! -e dist/$TARNAME ]; then
        echo dist/$TARNAME does not exist.  Try running setup.py sdist first.
        exit 1
    fi

    echo Uploading $PACKAGE...
    aws s3 cp --acl public-read dist/$TARNAME s3://dist.syapse.com/$PACKAGE_PATH/$PACKAGE
elif [ "$2" != "noupload" ]; then
    echo Second argument must either be not present or noupload
fi
# Generate the package_index.html by reading the file list from s3, filtering out package_index.html itself.
# In theory there's the problem of eventual consistency not getting the most recently uploaded file. If that happens,
# rerun with noupload.
{ echo '<html><head></head><body>'; aws s3 ls s3://dist.syapse.com/$PACKAGE_PATH/ | tr -s ' ' | cut -d ' ' -f 4 | grep -v package_index.html | sed -e 's|\(.*\)|<a href="\1">\1</a><br>|' ; echo '</body></html>'; } | \
    aws s3 cp --acl public-read --content-type text/html - s3://dist.syapse.com/$PACKAGE_PATH/package_index.html

# Invalidate the cached package_index.html from the dist.syapse.com CloudFront SSL cache
aws cloudfront create-invalidation --distribution-id E3A11MFJ8V9TOS --paths /$PACKAGE_PATH/package_index.html
