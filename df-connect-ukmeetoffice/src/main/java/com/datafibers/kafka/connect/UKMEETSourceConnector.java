/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.datafibers.kafka.connect;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UKMEETSourceConnector extends SourceConnector {
	private static final Logger log = LoggerFactory.getLogger(UKMEETSourceConnector.class);

	public static final String TOPIC_CONFIG = "topic";
	public static final String TOPIC_CONFIG_DOC = "The topic to publish data to";
	public static final String TOPIC_CONFIG_DEFAULT = "air_pressure";
	public static final String FILE_NAME_CONFIG = "prefix";
	public static final String FILE_NAME_CONFIG_DOC = "The file prefix name to process which is also equal to key target variable";
	public static final String FILE_NAME_CONFIG_DEFAULT = "surface_air_pressure";
	public static final String REFRESH_INTERVAL_CONFIG = "interval";
	public static final String REFRESH_INTERVAL_CONFIG_DOC = "How often to check stock update in seconds.";
	public static final String REFRESH_INTERVAL_CONFIG_DEFAULT = "20";
	public static final String SCHEMA_URI_CONFIG = "schema.registry.uri";
	public static final String SCHEMA_URI_CONFIG_DOC = "The URI to the Schema Registry.";
	public static final String SCHEMA_URI_CONFIG_DEFAULT = "http://localhost:8081";
	public static final String SCHEMA_SUBJECT_CONFIG = "schema.subject";
	public static final String SCHEMA_SUBJECT_CONFIG_DOC = "The subject used to validate avro schema.";
	public static final String SCHEMA_SUBJECT_CONFIG_DEFAULT = "n/a";
	public static final String PURGE_FLAG_CONFIG = "purge";
	public static final String PURGE_FLAG_CONFIG_DOC = "If delete message from aws sqs";
	public static final String PURGE_FLAG_CONFIG_DEFAULT = "y";

	public static final String SQS_URL_CONFIG = "sqs.url";
	public static final String SQS_URL_CONFIG_DOC = "aws sqs url";
	public static final String SQS_URL_CONFIG_DEFAULT = "https://sqs.us-east-2.amazonaws.com/520169828690/netcdf-queue";
	public static final String SQS_REGION_CONFIG = "sqs.region";
	public static final String SQS_REGION_CONFIG_DOC = "aws sqs region";
	public static final String SQS_REGION_CONFIG_DEFAULT = "us-east-1";

	public static final String S3_BUCKET_CONFIG = "s3.bucket";
	public static final String S3_BUCKET_CONFIG_DOC = "aws s3 bucket to get the file";
	public static final String S3_BUCKET_CONFIG_DEFAULT = "aws-earth-mo-atmospheric-mogreps-uk-prd";
	public static final String S3_REGION_CONFIG = "s3.region";
	public static final String S3_REGION_CONFIG_DOC = "aws s3 region";
	public static final String S3_REGION_CONFIG_DEFAULT = "eu-west-2";
	public static final String S3_DOWNDIR_CONFIG = "s3.downloaddir";
	public static final String S3_DOWNDIR_CONFIG_DOC = "local path where to keep the file downloaded from s3";
	public static final String S3_DOWNDIR_CONFIG_DEFAULT = "/tmp/s3downloaded";

	public static final String CUID = "cuid";
	public static final String CUID_DOC = "cuid";
	public static final String CUID_DEFAULT = "n/a";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(TOPIC_CONFIG, Type.STRING, TOPIC_CONFIG_DEFAULT, Importance.HIGH, TOPIC_CONFIG_DOC)
			.define(FILE_NAME_CONFIG, Type.STRING, FILE_NAME_CONFIG_DEFAULT, Importance.HIGH, FILE_NAME_CONFIG_DOC)
			.define(REFRESH_INTERVAL_CONFIG, Type.STRING, REFRESH_INTERVAL_CONFIG_DEFAULT, Importance.MEDIUM, REFRESH_INTERVAL_CONFIG_DOC)
			.define(SCHEMA_URI_CONFIG, Type.STRING, SCHEMA_URI_CONFIG_DEFAULT, Importance.HIGH, SCHEMA_URI_CONFIG_DOC)
			.define(SCHEMA_SUBJECT_CONFIG, Type.STRING, SCHEMA_SUBJECT_CONFIG_DEFAULT, Importance.HIGH, SCHEMA_SUBJECT_CONFIG_DOC)
			.define(PURGE_FLAG_CONFIG, Type.STRING, PURGE_FLAG_CONFIG_DEFAULT, Importance.LOW, PURGE_FLAG_CONFIG_DOC)
			.define(SQS_URL_CONFIG, Type.STRING, SQS_URL_CONFIG_DEFAULT, Importance.MEDIUM, SQS_URL_CONFIG_DOC)
			.define(SQS_REGION_CONFIG, Type.STRING, SQS_REGION_CONFIG_DEFAULT, Importance.MEDIUM, SQS_REGION_CONFIG_DOC)
			.define(S3_BUCKET_CONFIG, Type.STRING, S3_BUCKET_CONFIG_DEFAULT, Importance.MEDIUM, S3_BUCKET_CONFIG_DOC)
			.define(S3_REGION_CONFIG, Type.STRING, S3_REGION_CONFIG_DEFAULT, Importance.MEDIUM, S3_REGION_CONFIG_DOC)
			.define(S3_DOWNDIR_CONFIG, Type.STRING, S3_DOWNDIR_CONFIG_DEFAULT, Importance.MEDIUM, S3_DOWNDIR_CONFIG_DOC)
			.define(CUID, Type.STRING, CUID_DEFAULT, Importance.MEDIUM, CUID_DOC);

	private String topic;
	private String fileName;
	private String refreshInterval;
	private String schemaUri;
	private String schemaSubject;
	private String purgeFlag;
	private String sqsURL;
	private String sqsRegion;
	private String s3Bucket;
	private String s3Region;
	private String s3DownDir;
	private String cuid;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		topic = props.getOrDefault(TOPIC_CONFIG, TOPIC_CONFIG_DEFAULT);
		fileName = props.getOrDefault(FILE_NAME_CONFIG, FILE_NAME_CONFIG_DEFAULT);
		refreshInterval = props.getOrDefault(REFRESH_INTERVAL_CONFIG, REFRESH_INTERVAL_CONFIG_DEFAULT);
		schemaUri = props.getOrDefault(SCHEMA_URI_CONFIG, SCHEMA_URI_CONFIG_DEFAULT);
		schemaSubject = props.getOrDefault(SCHEMA_SUBJECT_CONFIG, SCHEMA_SUBJECT_CONFIG_DEFAULT);
		purgeFlag = props.getOrDefault(PURGE_FLAG_CONFIG, PURGE_FLAG_CONFIG_DEFAULT);
		sqsURL = props.getOrDefault(SQS_URL_CONFIG, SQS_URL_CONFIG_DEFAULT);
		sqsRegion = props.getOrDefault(SQS_REGION_CONFIG, SQS_REGION_CONFIG_DEFAULT);
		s3Bucket = props.getOrDefault(S3_BUCKET_CONFIG, S3_BUCKET_CONFIG_DEFAULT);
		s3Region = props.getOrDefault(S3_REGION_CONFIG, S3_REGION_CONFIG_DEFAULT);
		s3DownDir = props.getOrDefault(S3_DOWNDIR_CONFIG, S3_DOWNDIR_CONFIG_DEFAULT);
		cuid = props.getOrDefault(CUID, CUID_DEFAULT);

		if (topic == null)
			topic = TOPIC_CONFIG_DEFAULT;
		if (topic.contains(","))
			throw new ConnectException("UKMEETSourceConnector should only have a single topic when used as a source.");

		if (refreshInterval != null && !refreshInterval.isEmpty()) {
			try {
				Integer.parseInt(refreshInterval);
			} catch (NumberFormatException nfe) {
				throw new ConnectException("'interval' must be a valid integer");
			}
		} else {
			refreshInterval = "20";
		}

		if (schemaUri.endsWith("/"))
                schemaUri = schemaUri.substring(0, schemaUri.length() - 1);

		if (schemaSubject == null || schemaSubject.equalsIgnoreCase("n/a"))
                schemaSubject = topic;

		File dir = new File(s3DownDir);
		if(!dir.exists()) dir.mkdir();

	}

	@Override
	public Class<? extends Task> taskClass() {
		return UKMEETSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		Map<String, String> config = new HashMap<String, String>();
		config.put(TOPIC_CONFIG, topic);
		config.put(FILE_NAME_CONFIG, fileName);
		config.put(REFRESH_INTERVAL_CONFIG, refreshInterval);
		config.put(SCHEMA_URI_CONFIG, schemaUri);
		config.put(SCHEMA_SUBJECT_CONFIG, schemaSubject);
		config.put(PURGE_FLAG_CONFIG, purgeFlag);
		config.put(SQS_URL_CONFIG, sqsURL);
		config.put(SQS_REGION_CONFIG, sqsRegion);
		config.put(S3_BUCKET_CONFIG, s3Bucket);
		config.put(S3_REGION_CONFIG, s3Region);
		config.put(S3_DOWNDIR_CONFIG, s3DownDir);
		config.put(CUID, cuid);
		log.info("UKMEETSourceConnector value: {}", getValues(config));
		return Arrays.asList(config);
	}

	private String getValues(Map<String, String> config) {
		StringBuilder builder = new StringBuilder();
		for (String key : config.keySet()) {
			builder.append("\n\t").append(key).append(" = ").append(config.get(key));
		}
		return builder.append("\n").toString();
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

}
