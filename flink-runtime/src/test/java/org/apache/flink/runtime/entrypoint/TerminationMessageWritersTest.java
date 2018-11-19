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
import org.apache.flink.util.FileUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for {@link TerminationMessageWriters}.
 */
public class TerminationMessageWritersTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void writeToLocalFile() {
		File file = createTemporaryFile();
		TerminationMessageWriter writer = createTerminationMessageWriter(file);

		writer.writeTerminationMessage(TerminationMessage.SUCCEEDED);

		assertThat(readFile(file), is(equalTo("SUCCEEDED")));
	}

	@Test
	public void createNonExistingFile() {
		File file = new File(createTemporaryDirectory(), "non-existing");
		TerminationMessageWriter writer = createTerminationMessageWriter(file);

		writer.writeTerminationMessage(TerminationMessage.SUCCEEDED);

		assertThat(readFile(file), is(equalTo("SUCCEEDED")));
	}

	private File createTemporaryFile() {
		try {
			return tmp.newFile();
		} catch (IOException e) {
			throw new RuntimeException("Failed to create file", e);
		}
	}

	private File createTemporaryDirectory() {
		try {
			return tmp.newFolder();
		} catch (IOException e) {
			throw new RuntimeException("Failed to create directory", e);
		}
	}

	private static TerminationMessageWriter createTerminationMessageWriter(File file) {
		Configuration configuration = new Configuration();
		configuration.setString(ClusterEntrypoint.TERMINATION_MESSAGE_PATH, file.getAbsolutePath());
		return TerminationMessageWriters.forConfiguration(configuration);
	}

	private static String readFile(File file) {
		try {
			return FileUtils.readFileUtf8(file);
		} catch (IOException e) {
			throw new RuntimeException("Failed to read file", e);
		}
	}
}
