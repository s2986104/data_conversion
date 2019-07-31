#!/bin/sh

if ! swift auth > /dev/null ; then
    echo "Not Authenticated. Please load openstack rc file."
    exit 1
fi

BASEDIR="/mnt/collection/datasets/environmental/national-dynamic-land-cover"
SRCDIR="${BASEDIR}/layers"
CONTAINER="national-dynamic-land-cover_layers"

export RCLONE_CONFIG_REMOTE_TYPE=swift
export RCLONE_CONFIG_REMOTE_ENV_AUTH=true


rclone sync --progress "${SRCDIR}" "remote:${CONTAINER}"
