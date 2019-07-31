#!/bin/sh

if ! swift auth > /dev/null ; then
    echo "Not Authenticated. Please load openstack rc file."
    exit 1
fi

BASEDIR="/mnt/collection/datasets/environmental/vast"
SRCDIR="${BASEDIR}/layers"
CONTAINER="vast_layers"

export RCLONE_CONFIG_REMOTE_TYPE=swift
export RCLONE_CONFIG_REMOTE_ENV_AUTH=true


rclone sync --progress --include '*.json' "${SRCDIR}/" "remote:${CONTAINER}"
