/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.postgresql.replication.fluent.logical;

import org.postgresql.replication.fluent.CommonOptions;

import java.util.Properties;

public interface LogicalReplicationOptions extends CommonOptions {
  /**
   * Required parameter for logical replication.
   *
   * @return not null logical replication slot name that already exists on server and free.
   */
  String getSlotName();

  /**
   * Parameters for output plugin. Parameters will be set to output plugin that register for
   * specified replication slot name.
   *
   * @return list options that will be pass to output_plugin for that was create replication slot
   */
  Properties getSlotOptions();
}
