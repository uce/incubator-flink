/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * A {@link TerminationMessageWriter} factory that creates a writer for a configuration.
 */
class TerminationMessageWriters {

	static TerminationMessageWriter forConfiguration(Configuration configuration) {
		return getTerminationFilePath(configuration)
			.<TerminationMessageWriter>map(LocalFileTerminationMessageWriter::new)
			.orElse(NoopFileTerminationMessageWriter.INSTANCE);
	}

	private static Optional<String> getTerminationFilePath(Configuration configuration) {
		return ofNullable(configuration.getString(ClusterEntrypoint.TERMINATION_MESSAGE_PATH));
	}

	private static class LocalFileTerminationMessageWriter implements TerminationMessageWriter {

		private static final Logger LOG = LoggerFactory.getLogger(LocalFileTerminationMessageWriter.class);

		private final String terminationFilePath;

		private LocalFileTerminationMessageWriter(String terminationFilePath) {
			this.terminationFilePath = requireNonNull(terminationFilePath, "terminationFilePath");
		}

		@Override
		public void writeTerminationMessage(TerminationMessage message) {
			try {
				Files.write(
					new File(terminationFilePath).toPath(),
					message.name().getBytes(StandardCharsets.UTF_8),
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE);
			} catch (Throwable t) {
				LOG.error("Failed to write termination message to {}", terminationFilePath, t);
			}
		}
	}

	private enum NoopFileTerminationMessageWriter implements TerminationMessageWriter {
		INSTANCE;

		@Override
		public void writeTerminationMessage(TerminationMessage message) {
		}

	}

}
