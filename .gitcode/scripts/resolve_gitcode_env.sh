#!/usr/bin/env bash

# Copyright (c) 2026 Huawei Technologies Co., Ltd.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED.

set -euo pipefail

event_file="${ATOMGIT_EVENT_PATH:-${GITCODE_EVENT_PATH:-}}"
event_ref="${ATOMGIT_REF:-${GITCODE_REF:-}}"
pr_iid="${gitcodePullRequestIid:-${GITCODE_PR_IID:-}}"
target_branch="${gitcodeTargetBranch:-${GITCODE_TARGET_BRANCH:-master}}"
after_sha="${gitcodeAfterCommitSha:-${GITCODE_AFTER_COMMIT_SHA:-${GITCODE_SHA:-}}}"
merge_ref="${gitcodeMergeRef:-${GITCODE_MERGE_REF:-}}"

if [[ -n "${event_file}" && -f "${event_file}" ]]; then
  if [[ -z "${pr_iid}" ]]; then
    pr_iid="$(sed -n 's/.*"iid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "${event_file}" | head -n 1 || true)"
  fi
  if [[ -z "${pr_iid}" ]]; then
    pr_iid="$(sed -n 's/.*"number"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "${event_file}" | head -n 1 || true)"
  fi
  parsed_target="$(sed -n 's/.*"target_branch"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${event_file}" | head -n 1 || true)"
  if [[ -n "${parsed_target}" ]]; then
    target_branch="${parsed_target}"
  fi
fi

if [[ "${event_ref}" == refs/heads/* ]]; then
  target_branch="${event_ref#refs/heads/}"
fi

target_branch="${target_branch:-master}"

if [[ -n "${pr_iid}" && -z "${merge_ref}" ]]; then
  merge_ref="refs/merge-requests/${pr_iid}/merge"
fi

export GITCODE_PR_IID="${pr_iid}"
export GITCODE_TARGET_BRANCH="${target_branch}"
export GITCODE_AFTER_COMMIT_SHA="${after_sha}"
export GITCODE_MERGE_REF="${merge_ref}"

env_file="${ATOMGIT_ENV:-${GITCODE_ENV:-}}"
if [[ -n "${env_file}" ]]; then
  {
    echo "GITCODE_PR_IID=${GITCODE_PR_IID}"
    echo "GITCODE_TARGET_BRANCH=${GITCODE_TARGET_BRANCH}"
    echo "GITCODE_AFTER_COMMIT_SHA=${GITCODE_AFTER_COMMIT_SHA}"
    echo "GITCODE_MERGE_REF=${GITCODE_MERGE_REF}"
  } >> "${env_file}"
fi

echo "Resolved GitCode env:"
echo "  GITCODE_PR_IID=${GITCODE_PR_IID}"
echo "  GITCODE_TARGET_BRANCH=${GITCODE_TARGET_BRANCH}"
echo "  GITCODE_AFTER_COMMIT_SHA=${GITCODE_AFTER_COMMIT_SHA}"
echo "  GITCODE_MERGE_REF=${GITCODE_MERGE_REF}"
