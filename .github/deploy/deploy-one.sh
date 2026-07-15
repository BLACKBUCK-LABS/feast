#!/usr/bin/env bash
# Deploy Feast serving to ONE EC2 box via SSM: git pull, build with Maven ON THE BOX (matches
# the manual steps already run there), restart via Supervisor, then block until the gRPC health
# endpoint reports SERVING.
#
# Usage: deploy-one.sh <instance-id> <branch>
#
# Health-gating: does not return success until grpc.health.v1.Health/Check reports SERVING, so
# box N+1 (which `needs` box N) never starts while box N is still restarting/loading the registry.
set -euo pipefail

INSTANCE_ID="$1"
BRANCH="$2"

REPO_DIR="/var/www/feast"
GRPC_PORT="6566"
SUPERVISOR_PROGRAM="feast_serving"

# SSM runs /bin/sh (dash); base64+bash so dash never parses this bash script (loops, pipefail).
read -r -d '' REMOTE <<EOF || true
set -euo pipefail
cd "${REPO_DIR}"
git fetch origin "${BRANCH}"
git checkout "${BRANCH}"
git reset --hard "origin/${BRANCH}"

# System mvn is 3.3.9 here, but the enforcer plugin (maven-enforcer-plugin) requires >=3.6.
# apache-maven-3.8.7-bin.tar.gz sits next to the repo for exactly that reason - unpack once if
# needed, then use it explicitly instead of relying on PATH.
MAVEN_HOME_DIR="${REPO_DIR}/apache-maven-3.8.7"
if [ ! -x "\${MAVEN_HOME_DIR}/bin/mvn" ] && [ -f "${REPO_DIR}/apache-maven-3.8.7-bin.tar.gz" ]; then
  tar -xzf "${REPO_DIR}/apache-maven-3.8.7-bin.tar.gz" -C "${REPO_DIR}"
fi
MVN="\${MAVEN_HOME_DIR}/bin/mvn"
if [ ! -x "\${MVN}" ]; then
  echo "apache-maven-3.8.7 not found/unpacked - falling back to system mvn"
  MVN="mvn"
fi
echo "Using \$("\${MVN}" -version | head -1)"

"\${MVN}" -f java/pom.xml install -Dmaven.test.skip=true
"\${MVN}" -f java/serving/pom.xml package -Dmaven.test.skip=true

sudo supervisorctl restart ${SUPERVISOR_PROGRAM}

# Health gate: poll up to ~150s via the gRPC health-checking protocol. The app reloads the
# registry + reconnects to Redis on boot, so grpcurl returning "connection refused" for the
# first several iterations is expected, NOT a failure. errexit/pipefail are disabled for the
# poll so one failing grpcurl call can't kill the loop.
set +e
healthy=0
for i in \$(seq 1 50); do
  status=\$(grpcurl -plaintext -d '{}' "localhost:${GRPC_PORT}" grpc.health.v1.Health/Check 2>/dev/null | grep -o 'SERVING')
  if [ "\$status" = "SERVING" ]; then
    echo "HEALTHY after ~\$((i*3))s"; healthy=1; break
  fi
  sleep 3
done
if [ "\$healthy" -ne 1 ]; then
  echo "NOT HEALTHY after ~150s — failing deploy"
  sudo supervisorctl tail -3000 ${SUPERVISOR_PROGRAM} stderr || true
  exit 1
fi
exit 0
EOF

REMOTE_B64=$(printf '%s' "${REMOTE}" | base64 | tr -d '\n')
RUN_CMD="echo ${REMOTE_B64} | base64 -d | bash"
PARAMS=$(jq -nc --arg c "${RUN_CMD}" '{commands: [$c]}')

echo "=== Sending deploy command to ${INSTANCE_ID} ==="
CMD_ID=$(aws ssm send-command \
  --instance-ids "${INSTANCE_ID}" \
  --document-name "AWS-RunShellScript" \
  --comment "deploy feast-serving ${GITHUB_SHA:-manual}" \
  --parameters "${PARAMS}" \
  --timeout-seconds 1800 \
  --query "Command.CommandId" --output text)
echo "SSM Command ID: ${CMD_ID}"

# The CLI's built-in `command-executed` waiter gives up after ~100s (5s delay x 20 attempts),
# far short of a cold-cache Maven build. Poll manually up to the SSM command timeout instead.
echo "=== Waiting for command to finish (up to 30 min) ==="
for i in $(seq 1 180); do
  CUR_STATUS=$(aws ssm get-command-invocation --command-id "${CMD_ID}" --instance-id "${INSTANCE_ID}" \
    --query "Status" --output text 2>/dev/null || echo "Pending")
  case "${CUR_STATUS}" in
    Success|Failed|Cancelled|TimedOut) break ;;
  esac
  sleep 10
done

echo "=== Output from ${INSTANCE_ID} ==="
aws ssm get-command-invocation --command-id "${CMD_ID}" --instance-id "${INSTANCE_ID}" \
  --query "{Status:Status, Out:StandardOutputContent, Err:StandardErrorContent}" --output json

STATUS=$(aws ssm get-command-invocation --command-id "${CMD_ID}" --instance-id "${INSTANCE_ID}" \
  --query "Status" --output text)
echo "Status: ${STATUS}"
if [ "${STATUS}" != "Success" ]; then
  echo "::error::Deploy/health-check failed on ${INSTANCE_ID}"
  exit 1
fi
echo "${INSTANCE_ID} feast-serving deployed and healthy."
