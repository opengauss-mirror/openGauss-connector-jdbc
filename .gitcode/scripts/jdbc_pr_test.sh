#!/usr/bin/env bash
set -euo pipefail

repo_url="${JDBC_REPO_URL:-https://gitcode.com/opengauss/openGauss-connector-jdbc.git}"
workspace_root="${WORKSPACE_ROOT:-/var/jenkins_home/workspace}"
workdir="${JDBC_WORKDIR:-${workspace_root%/}/jdbc}"
db_user="${DB_INIT_USER:-jdbc_jenkins}"
gauss_env="${GAUSS_ENV_PATH:-/home/jdbc_jenkins/gauss_env}"
maven_cmd="${MAVEN_CMD:-mvn}"

setup_java() {
  export JAVA_HOME="${JDBC_JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-5.oe2203.aarch64}"
  if [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
    echo "JDK 8 is not available at ${JAVA_HOME}" >&2
    return 1
  fi
  export PATH="${JAVA_HOME}/bin:${PATH}"
  java -version
}

drop_caches() {
  sync || true
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    echo 3 > /proc/sys/vm/drop_caches || true
  elif command -v sudo >/dev/null 2>&1; then
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null || true
  fi
  ipcrm -a || true
}

restart_cluster() {
  if [[ ! -f "${gauss_env}" ]]; then
    echo "Skip openGauss restart: ${gauss_env} does not exist."
    return 0
  fi

  if id "${db_user}" >/dev/null 2>&1; then
    su - "${db_user}" <<EOF
set -e
source "${gauss_env}"
gs_om -t restart
EOF
  else
    source "${gauss_env}"
    gs_om -t restart
  fi
}

validate_target_branch() {
  local branch="${GITCODE_TARGET_BRANCH:-master}"
  if [[ "${branch}" != "master" && "${branch}" != "6.0.0" ]]; then
    echo "branch is not master/6.0.0, skip"
    exit 0
  fi
}

clone_with_retry() {
  local branch="${GITCODE_TARGET_BRANCH:-master}"
  local attempt=0

  rm -rf "${workdir}"
  while [[ "${attempt}" -lt 10 ]]; do
    echo "${attempt}"
    rm -rf "${workdir}"
    if timeout 120 git clone "${repo_url}" -b "${branch}" "${workdir}"; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 5
  done

  echo "Failed to clone ${repo_url} branch ${branch}" >&2
  return 1
}

merge_pull_request() {
  cd "${workdir}"
  git rev-parse --is-inside-work-tree
  git config remote.origin.url "${repo_url}"

  if [[ -n "${GITCODE_PR_IID:-}" ]]; then
    local merge_ref="${GITCODE_MERGE_REF:-refs/merge-requests/${GITCODE_PR_IID}/merge}"
    git fetch --tags --force --progress origin "${merge_ref}:${merge_ref}"
    git checkout -b "${merge_ref}" "${merge_ref}"
  elif [[ -n "${GITCODE_AFTER_COMMIT_SHA:-}" ]]; then
    git fetch --tags --force --progress origin "${GITCODE_AFTER_COMMIT_SHA}"
    git checkout -f "${GITCODE_AFTER_COMMIT_SHA}"
  else
    git checkout -f "${GITCODE_TARGET_BRANCH:-master}"
  fi
}

write_build_properties() {
  if [[ -z "${JDBC_DB_PASSWORD:-}" ]]; then
    echo "JDBC_DB_PASSWORD is required" >&2
    return 1
  fi

  cat > "${workdir}/build.properties" <<EOF
server=${JDBC_DB_HOST:-localhost}
port=${JDBC_DB_PORT:-15432}
secondaryServer=${JDBC_SECONDARY_DB_HOST:-localhost}
secondaryPort=${JDBC_SECONDARY_DB_PORT:-15432}
secondaryServer2=${JDBC_THIRD_DB_HOST:-localhost}
secondaryServerPort2=${JDBC_THIRD_DB_PORT:-15432}
database=${JDBC_DB_NAME:-target_db_a}
database_b=${JDBC_DB_NAME_B:-target_db_b}
database_pg=${JDBC_DB_NAME_PG:-target_db_pg}
username=${JDBC_DB_USER:-test_case_user}
password=${JDBC_DB_PASSWORD}
loggerLevel=OFF
sslpassword=sslpwd
EOF
}

run_tests() {
  cd "${workdir}"
  echo "start jdbc testCase"
  "${maven_cmd}" test '-Dtest=org.postgresql.**.*'
  echo "jdbc testCase success"
}

echo "gitcodePullRequestIid: ${GITCODE_PR_IID:-}"
echo "gitcodeTargetBranch: ${GITCODE_TARGET_BRANCH:-master}"
echo "gitcodeAfterCommitSha: ${GITCODE_AFTER_COMMIT_SHA:-}"
echo "gitcodeRef: ${GITCODE_MERGE_REF:-}"

git config --global core.compression 0
setup_java
drop_caches
validate_target_branch
restart_cluster
clone_with_retry
merge_pull_request
write_build_properties
run_tests
