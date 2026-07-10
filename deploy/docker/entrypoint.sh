#!/bin/sh
set -e

if [ -z "${NODE_ID}" ]; then
  echo "NODE_ID is required (e.g. spec.nodeName from downward API)" >&2
  exit 1
fi

set -- run \
  --node-id="${NODE_ID}" \
  --cluster="${ZTM_CLUSTER:-default}" \
  --data-dir="${ZTM_DATA_DIR:-/var/lib/ztm}" \
  --mesh-bind="${ZTM_MESH_BIND:-:7444}" \
  --client-bind="${ZTM_CLIENT_BIND:-:7443}" \
  --gossip-bind="${ZTM_GOSSIP_BIND:-:7946}" \
  --admin-bind="${ZTM_ADMIN_BIND:-0.0.0.0:8080}" \
  --rpc-bind="${ZTM_RPC_BIND:-0.0.0.0:9090}"

if [ -n "${ZTM_JOIN}" ]; then
  set -- "$@" --join="${ZTM_JOIN}"
fi

if [ -n "${ZTM_ALLOW_SERVICE}" ]; then
  set -- "$@" --allow-service="${ZTM_ALLOW_SERVICE}"
fi

exec ztm-node "$@"
