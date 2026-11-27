#!/bin/bash -x

# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All rights reserved.
#
# Licensed under the 2-Clause BSD License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://opensource.org/license/bsd-2-clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script should be run from pgjdbc clone, and it deploys pgjdbc, pgjdbc-jre7, and pgjdbc-jre6 to Staging repository
# Later the staged repositories can be inspected or promoted
# Note: if script fails in the middle, manual git reset, and removal of the tag might be required
# Note: the script should be run like ./release_stage.sh 42.1.1 42.1.2

RELEASE_VERSION=$1
NEXT_VERSION=$2

if [ -z "$RELEASE_VERSION" ]; then
  echo "Release version is not set"
  exit 1
fi
if [ -z "$NEXT_VERSION" ]; then
  echo "Next version is not set"
  exit 1
fi

function clone_prev {
  JRE=$1
  if [ -d pgjdbc-jre$JRE ]; then
    echo pgjdbc-jre$JRE already exists
    return
  fi
  echo Cloning pgjdbc-jre$JRE
  echo git clone https://github.com/pgjdbc/pgjdbc-jre$JRE
  echo cd pgjdbc-jre$JRE
  echo git submodule update --init
}

(clone_prev 6)
(clone_prev 7)

function mvn_release {
  SUFFIX=$1
  if [ -f "released_$SUFFIX" ]
  then
    echo "$SUFFIX has already been released"
    return
  fi
  echo
  echo
  echo Releasing $SUFFIX
  echo =================
  echo

  git reset --hard
  mvn release:clean release:prepare -DreleaseVersion=$RELEASE_VERSION$SUFFIX -DdevelopmentVersion=$NEXT_VERSION$SUFFIX-SNAPSHOT -Dtag=REL$RELEASE_VERSION$SUFFIX &&\
  mvn release:perform &&\
  echo https://oss.sonatype.org/content/repositories/staging/org/postgresql/postgresql/$RELEASE_VERSION$SUFFIX/ > released_$SUFFIX
}

function release_prev {
  JRE=$1
  cd pgjdbc-$JRE/pgjdbc &&\
  git fetch &&\
  git reset --hard REL$RELEASE_VERSION &&\
  cd .. &&\
  git checkout master &&\
  git reset --hard origin/master &&\
  git add pgjdbc &&\
  git commit -m "Update pgjdbc to $RELEASE_VERSION" &&\
  mvn_release .$JRE
}

mvn_release &&\
(release_prev jre7) &&\
(release_prev jre6)
