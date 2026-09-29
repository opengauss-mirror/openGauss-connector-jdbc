#!/usr/bin/env bash
set -euo pipefail

repo_url="${JDBC_REPO_URL:-https://gitcode.com/opengauss/openGauss-connector-jdbc.git}"
workspace_root="${WORKSPACE_ROOT:-/var/jenkins_home/workspace}"
workdir="${JDBC_WORKDIR:-${workspace_root%/}/jdbc}"
db_user="${DB_INIT_USER:-jdbc_jenkins}"
gauss_env="${GAUSS_ENV_PATH:-/home/jdbc_jenkins/gauss_env}"
maven_cmd="${MAVEN_CMD:-mvn}"

setup_java() {
  if [[ -n "${JDBC_JAVA_HOME:-}" && -x "${JDBC_JAVA_HOME}/bin/java" ]]; then
    export JAVA_HOME="${JDBC_JAVA_HOME}"
  elif [[ -x /usr/local/jdk1.8.0_412/bin/java ]]; then
    export JAVA_HOME=/usr/local/jdk1.8.0_412
  elif [[ -x /usr/local/jdk8/bin/java ]]; then
    export JAVA_HOME=/usr/local/jdk8
  elif [[ -x /usr/local/java8/bin/java ]]; then
    export JAVA_HOME=/usr/local/java8
  fi

  if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="${JAVA_HOME}/bin:${PATH}"
  fi

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
gs_om -t stop
gs_om -t start
EOF
  else
    source "${gauss_env}"
    gs_om -t stop
    gs_om -t start
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
  git config user.email "${GIT_AUTHOR_EMAIL:-gitcode-actions@users.noreply.gitcode.com}"
  git config user.name "${GIT_AUTHOR_NAME:-gitcode-actions}"

  if [[ -n "${GITCODE_PR_IID:-}" ]]; then
    local pr_head="refs/merge-requests/${GITCODE_PR_IID}/head"
    git fetch --force --progress origin "${pr_head}:${pr_head}"
    git merge --no-verify "${pr_head}" --no-edit
  elif [[ -n "${GITCODE_AFTER_COMMIT_SHA:-}" ]]; then
    git fetch --tags --force --progress origin "${GITCODE_AFTER_COMMIT_SHA}"
    git checkout -f "${GITCODE_AFTER_COMMIT_SHA}"
  else
    git checkout -f "${GITCODE_TARGET_BRANCH:-master}"
  fi
}

write_local_properties() {
  local props="${workdir}/build.local.properties"
  {
    echo "server=${JDBC_DB_HOST:-localhost}"
    echo "port=${JDBC_DB_PORT:-5432}"
    echo "secondaryServer=${JDBC_SECONDARY_DB_HOST:-localhost}"
    echo "secondaryPort=${JDBC_SECONDARY_DB_PORT:-5433}"
    echo "secondaryServer2=${JDBC_THIRD_DB_HOST:-localhost}"
    echo "secondaryServerPort2=${JDBC_THIRD_DB_PORT:-5434}"
    echo "database=${JDBC_DB_NAME:-jdbc_utf8_a}"
    echo "database_pg=${JDBC_DB_NAME_PG:-jdbc_utf8_pg}"
    echo "database_b=${JDBC_DB_NAME_B:-jdbc_utf8_b}"
    echo "username=${JDBC_DB_USER:-test}"
    echo "password=${JDBC_DB_PASSWORD:-test123@}"
    echo "privilegedUser=${JDBC_DB_PRIVILEGED_USER:-postgres}"
    echo "privilegedPassword=${JDBC_DB_PRIVILEGED_PASSWORD:-}"
    echo "loggerFile=${JDBC_LOGGER_FILE:-target/pgjdbc-tests.log}"
  } > "${props}"
}

run_tests() {
  cd "${workdir}"
  echo "start jdbc testCase"
  "${maven_cmd}" -B test
  echo "jdbc testCase success"
}

echo "gitcodePullRequestIid: ${GITCODE_PR_IID:-}"
echo "gitcodeTargetBranch: ${GITCODE_TARGET_BRANCH:-master}"
echo "gitcodeAfterCommitSha: ${GITCODE_AFTER_COMMIT_SHA:-}"
echo "gitcodeRef: ${GITCODE_MERGE_REF:-}"

git config --global core.compression 0
setup_java
drop_caches
restart_cluster
clone_with_retry
merge_pull_request
write_local_properties
run_tests
