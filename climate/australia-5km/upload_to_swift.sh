#!/bin/bash

if ! swift auth > /dev/null ; then
    echo "Not Authenticated. Please load openstack rc file."
    exit 1
fi

SRCDIR="bccvl/layers"
CONTAINER="australia_5km_layers"

export RCLONE_CONFIG_REMOTE_TYPE=swift
export RCLONE_CONFIG_REMOTE_ENV_AUTH=true


rclone sync --progress "${SRCDIR}" "remote:${CONTAINER}"
