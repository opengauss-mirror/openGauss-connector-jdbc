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

import org.postgresql.replication.fluent.ChainedCommonCreateSlotBuilder;

/**
 * Logical replication slot specific parameters.
 */
public interface ChainedLogicalCreateSlotBuilder
    extends ChainedCommonCreateSlotBuilder<ChainedLogicalCreateSlotBuilder> {

  /**
   * <p>Output plugin that should be use for decode physical represent WAL to some logical form.
   * Output plugin should be installed on server(exists in shared_preload_libraries).</p>
   *
   * <p>Package postgresql-contrib provides sample output plugin <b>test_decoding</b> that can be
   * use for test logical replication api</p>
   *
   * @param outputPlugin not null name of the output plugin used for logical decoding
   * @return the logical slot builder
   */
  ChainedLogicalCreateSlotBuilder withOutputPlugin(String outputPlugin);
}
