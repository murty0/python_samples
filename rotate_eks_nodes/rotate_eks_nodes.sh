#!/bin/bash

set -eoux pipefail

REGION="$1"
TARGET="$2"

declare -a targets_array
targets_array=('dbs' 'apps' 'all')

if [ -z "$REGION" ] || [ -z "$TARGET" ] ; then
  echo "usage: ./rotate_eks_nodes [region] [dbs|apps|all]"
  exit
fi

if ! [[ " ${targets_array[*]} " == *" $TARGET "* ]]; then
   echo "only options allowed are: [dbs|apps|all]"
   exit
fi

kubectl config use-context "arn:aws:eks:$REGION:123456789:cluster/$REGION-eks"
python scripts/rotate_eks_nodes.py "$REGION" "$TARGET"
