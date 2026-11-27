/*
 * Copyright (c) 2014, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.postgresql.hostchooser;

import org.postgresql.util.HostSpec;

import java.util.Properties;

/**
 * Chooses a {@link HostChooser} instance based on the number of hosts and properties.
 */
public class HostChooserFactory {

    public static HostChooser createHostChooser(HostSpec[] hostSpecs,
                                                HostRequirement targetServerType, Properties info) {
        if (hostSpecs.length == 1) {
            return new SingleHostChooser(hostSpecs[0], targetServerType);
        }
        return new MultiHostChooser(hostSpecs, targetServerType, info);
    }
}
