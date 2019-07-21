/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.datafibers.kafka.connect;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaParseException;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayLong;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class UKMEETSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(UKMEETSourceTask.class);

    public static final String KEY_FIELD = "symbol";
    public static final String VALUE_FIELD = "refresh_time";
    public static final String HTTP_HEADER_APPLICATION_JSON_CHARSET = "application/json; charset=utf-8";
    public static final String AVRO_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    public static final int SQS_TIME_OUT = 10;
    public static final int SQS_MESSASGE_POLL_RATE= 1; // Max is 10

    private String topic;
    private String fileName;
    private int interval;
    private String purgeFlag;
    private String schemaUri;
    private Schema dataSchema;

    private AmazonSQS sqs;
    private String sqsURL;
    private Regions sqsRegion;

    private AmazonS3 s3client;
    private Regions s3Region;
    private String s3Bucket;
    private String s3DownloadDir;

    private AWSCredentials awsCredentials;

    private String cuid;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(UKMEETSourceConnector.TOPIC_CONFIG);
        fileName = props.get(UKMEETSourceConnector.FILE_NAME_CONFIG);
        interval = Integer.parseInt(props.get(UKMEETSourceConnector.REFRESH_INTERVAL_CONFIG)) * 1000;
        purgeFlag = props.get(UKMEETSourceConnector.PURGE_FLAG_CONFIG);
        cuid = props.get(UKMEETSourceConnector.CUID);
        schemaUri = props.get(UKMEETSourceConnector.SCHEMA_URI_CONFIG);

        awsCredentials = new BasicAWSCredentials(
                System.getenv("AWS_ACCESS_KEY_ID"),
                System.getenv("AWS_SECRET_ACCESS_KEY"));

        sqsURL = props.get(UKMEETSourceConnector.SQS_URL_CONFIG);
        sqsRegion = Regions.fromName(props.get(UKMEETSourceConnector.SQS_REGION_CONFIG));
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(sqsRegion)
                .build();

        s3Bucket = props.get(UKMEETSourceConnector.S3_BUCKET_CONFIG);
        s3DownloadDir = props.get(UKMEETSourceConnector.S3_DOWNDIR_CONFIG);
        s3Region = Regions.fromName(props.get(UKMEETSourceConnector.S3_REGION_CONFIG));
        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(s3Region)
                .build();


        // TODO create a stock schema and write to the schema registry
        addSchemaIfNotAvailable(topic);
        dataSchema = getBuildSchema(schemaUri, topic, "latest");
    }

    /**
     * Poll data from AWS SQS
     *
     * @return
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();
        try {
                int i = 0;
                // Poll one target file only
                while(true) {
                    ReceiveMessageRequest receiveMessageRequest =
                            new ReceiveMessageRequest(sqsURL)
                                    .withWaitTimeSeconds(SQS_TIME_OUT)
                                    .withMaxNumberOfMessages(SQS_MESSASGE_POLL_RATE);

                    List<Message> sqsMessages = sqs.receiveMessage(receiveMessageRequest).getMessages();

                    final Message message = sqsMessages.get(0);
                    //System.out.println(new JSONObject(message.getBody()).getString("Message").replace("\\\\", ""));
                    JSONObject msg = new JSONObject(
                            new JSONObject(message.getBody())
                                    .getString("Message").replace("\\\\", ""));
                    //System.out.println("bucket=" + msg.getString("bucket"));
                    //System.out.println("file_name=" + msg.getString("name"));
                    //System.out.println("file_key=" + msg.getString("key"));
                    //System.out.println("file_created_time=" + msg.getString("created_time"));
                    //System.out.println("forecast_reference_time=" + msg.getString("forecast_reference_time"));
                    //System.out.println("index=" + i);
                    if (msg.getString("name").equalsIgnoreCase(fileName)) {
                        //downloading an object to file
                        String fileCreationTime = msg.getString("created_time");
                        String fileLongName = msg.getString("name") + "_" +
                                fileCreationTime + "_" + msg.getString("key");
                        String filePathName = s3DownloadDir + "/" + fileLongName;
                        S3Object s3object = s3client.getObject(s3Bucket, msg.getString("key"));
                        S3ObjectInputStream inputStream = s3object.getObjectContent();

                        FileUtils.copyInputStreamToFile(inputStream, new File(filePathName));
                        log.info(fileLongName + " is downloaded and start processing ");

                        NetcdfFile dataFile = NetcdfFile.open(filePathName, null);

                        // read the target variables
                        Variable pressureVar = dataFile.findVariable(fileName); // Use fileName(Prefix) to match KPI TODO: Improve
                        Variable yVar = dataFile.findVariable("projection_y_coordinate");
                        Variable xVar = dataFile.findVariable("projection_x_coordinate");

                        // Read the valid time and forecast time
                        Variable time = dataFile.findVariable("time");
                        ArrayLong.D0 timeCoord = (ArrayLong.D0) time.read();

                        Variable forecastTime = dataFile.findVariable("forecast_reference_time");
                        ArrayLong.D0 forecastTimeCoord = (ArrayLong.D0) forecastTime.read();

                        // Read the latitude and longitude coordinate variables into arrays
                        ArrayFloat.D1 yCoord = (ArrayFloat.D1) yVar.read();
                        ArrayFloat.D1 xCoord = (ArrayFloat.D1) xVar.read();

                        // Read the pressure data
                        ArrayFloat.D3 pressure = (ArrayFloat.D3) pressureVar.read();

                        // dimensions of the y/x array
                        List<Dimension> dimensions = dataFile.getDimensions();
                        int yLength = dimensions.get(1).getLength(); // index 0 is for realization
                        int xLength = dimensions.get(2).getLength();

                        String msgToKafka;

                        // iterate through the arrays, do something with the data
                        for (int y = 0; y < yLength; y++) {
                            for (int x = 0; x < xLength; x++) {
                                // do something with the data
                                msgToKafka = new JSONObject()
                                        .put("file_prefix", fileName)
                                        .put("file_name", fileLongName)
                                        .put("file_creation", fileCreationTime)
                                        .put("x_coord", xCoord.get(x))
                                        .put("y_coord", yCoord.get(y))
                                        .put("time", timeCoord.get())
                                        .put("forecast", forecastTimeCoord.get())
                                        .put("pressure", pressure.get(0, y, x)).toString();

                                records.add(
                                        new SourceRecord(offsetKey(fileLongName),
                                        offsetValue("y=" + yLength + ", x=" + xLength), topic,
                                        dataSchema, structDecodingFromJson(msgToKafka)));
                                log.info("Sending Kafka message =" + msgToKafka);
                            }
                        }
                        if(!purgeFlag.equalsIgnoreCase("false")) {
                            sqs.deleteMessage(new DeleteMessageRequest(sqsURL, message.getReceiptHandle()));
                            FileUtils.forceDelete(new File(fileLongName));
                            log.info("The message/temp file are deleted from SQS/" + s3DownloadDir);
                        }
                        break;
                    }
                }

        } catch (IOException ioe) {
                ioe.printStackTrace();
        }

        Thread.sleep(interval);
        return records;
    }

    @Override
    public void stop() {
    }

    private SchemaBuilder getInnerBuilder(Field field) {
        return getInnerBuilder(field, field.schema().getType());
    }

    private SchemaBuilder getInnerBuilder(Field field, Type type) {
        boolean hasDefault = field.defaultValue() != null;
        SchemaBuilder innerBuilder = null;
        switch (type) {
            case STRING: {
                innerBuilder = SchemaBuilder.string();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asText());
                break;
            }
            case INT: {
                innerBuilder = SchemaBuilder.int32();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asInt());
                break;
            }
            case LONG: {
                innerBuilder = SchemaBuilder.int64();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asLong());
                break;
            }
            case DOUBLE: {
                innerBuilder = SchemaBuilder.float64();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asDouble());
                break;
            }
            case FLOAT:{
                innerBuilder = SchemaBuilder.float32();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asDouble());
                break;
            }
            case BOOLEAN: {
                innerBuilder = SchemaBuilder.bool();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asBoolean());
                break;
            }
            case UNION: {
                for (org.apache.avro.Schema schema : field.schema().getTypes()) {
                    if (!schema.getType().equals(NULL))
                        return getInnerBuilder(field, schema.getType());
                }
            }
            default:
                throw new ConnectException(
                        "Unable to build schema because there is no case for type " + field.schema().getType());
        }
        return innerBuilder;
    }

    private Map<String, String> offsetKey(String keyName) {
        return Collections.singletonMap(KEY_FIELD, keyName);
    }

    private Map<String, String> offsetValue(String value) {
        return Collections.singletonMap(VALUE_FIELD, value);
    }

    /**
     * Decode Json to struct according to schema form Confluent schema registry
     * @param line
     * @return struct of decoded
     */
    public Struct structDecodingFromJson(String line) {

        if (line.length() > 0) {
            JsonNode json = null;
            try {
                json = new ObjectMapper().readValue(line, JsonNode.class);
            } catch (IOException ex) {
                throw new ConnectException(String.format("Unable to parse %s into a valid JSON"), ex);
            }

            Struct struct = new Struct(dataSchema);
            Iterator<Entry<String, JsonNode>> iterator = json.getFields();
            while (iterator.hasNext()) {
                Entry<String, JsonNode> entry = iterator.next();
                Object value = null;
                org.apache.kafka.connect.data.Field theField = dataSchema.field(entry.getKey());
                if (theField != null) {
                    switch (theField.schema().type()) {
                        case STRING:
                        case BYTES: {
                            value = entry.getValue().asText();
                            break;
                        }
                        case INT32: {
                            value = entry.getValue().asInt();
                            break;
                        }
                        case INT64: {
                            value = entry.getValue().asLong();
                            break;
                        }
                        case FLOAT32:
                        case FLOAT64: {
                            value = entry.getValue().asDouble();
                            break;
                        }
                        case BOOLEAN: {
                            value = entry.getValue().asBoolean();
                            break;
                        }
                        default:
                            value = entry.getValue().asText();
                    }
                }
                System.out.println("STRUCT PUT -" + entry.getKey() + ":" + value);
                struct.put(entry.getKey(), value);

            }
            return struct;
        }
        return null;
    }

    /**
     * Utility to get the schema from schema registry
     * @param schemaUri
     * @param schemaSubject
     * @param schemaVersion
     * @return
     */
    private Schema getBuildSchema(String schemaUri, String schemaSubject, String schemaVersion) {

        String fullUrl = String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

        String schemaString = null;
        BufferedReader br = null;

        try {
            StringBuilder response = new StringBuilder();
            String line;
            br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));

            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
            schemaString = responseJson.get("schema").asText();
            log.info("Schema String is " + schemaString);
        } catch (Exception ex) {
            throw new ConnectException("Unable to retrieve schema from Schema Registry", ex);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        org.apache.avro.Schema avroSchema = null;
        try {
            avroSchema = new Parser().parse(schemaString);
        } catch (SchemaParseException ex) {
            throw new ConnectException(
                    String.format("Unable to successfully parse schema from: %s", schemaString), ex);
        }

        SchemaBuilder builder;
        // TODO: Add other avro schema types
        switch (avroSchema.getType()) {
            case RECORD: {
                builder = SchemaBuilder.struct();
                break;
            }
            case STRING: {
                builder = SchemaBuilder.string();
                break;
            }
            case INT: {
                builder = SchemaBuilder.int32();
                break;
            }
            case LONG: {
                builder = SchemaBuilder.int64();
                break;
            }
            case FLOAT: {
                builder = SchemaBuilder.float32();
                break;
            }
            case DOUBLE: {
                builder = SchemaBuilder.float64();
                break;
            }
            case BOOLEAN: {
                builder = SchemaBuilder.bool();
                break;
            }
            case BYTES: {
                builder = SchemaBuilder.bytes();
                break;
            }
            default:
                builder = SchemaBuilder.string();
        }

        if (avroSchema.getFullName() != null)
            builder.name(avroSchema.getFullName());
        if (avroSchema.getDoc() != null)
            builder.doc(avroSchema.getDoc());

        if (RECORD.equals(avroSchema.getType()) && avroSchema.getFields() != null
                && !avroSchema.getFields().isEmpty()) {
            for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                boolean hasDefault = field.defaultValue() != null;

                SchemaBuilder innerBuilder = getInnerBuilder(field);

                if (hasDefault)
                    innerBuilder.optional();

                builder.field(field.name(), innerBuilder.build());
            }
        }

        return builder.build();
    }

    private void addSchemaIfNotAvailable(String subject) {

        String schemaRegistryRestURL = schemaUri + "/subjects/" + subject + "/versions";

        try {
            HttpResponse<String> schemaRes = Unirest.get(schemaRegistryRestURL + "/latest")
                    .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if(schemaRes.getStatus() == 404) { // Add the meta sink schema
                Unirest.post(schemaRegistryRestURL)
                        .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                        .header("Content-Type", AVRO_REGISTRY_CONTENT_TYPE)
                        .body(new JSONObject().put("schema",
                                "{\"type\":\"record\"," +
                                        "\"name\": \"" + subject + "\"," +
                                        "\"fields\":[" +
                                        "{\"name\": \"file_prefix\", \"type\":\"string\"}," +
                                        "{\"name\": \"file_name\", \"type\":\"string\"}," +
                                        "{\"name\": \"file_creation\", \"type\":\"string\"}," +
                                        "{\"name\": \"x_coord\", \"type\": \"double\"}," +
                                        "{\"name\": \"y_coord\", \"type\": \"double\"}, " +
                                        "{\"name\": \"time\", \"type\": \"double\"}," +
                                        "{\"name\": \"forecast\", \"type\": \"double\"}," +
                                        "{\"name\": \"pressure\", \"type\": \"double\"}]}"
                                ).toString()
                        ).asString();
                log.info("Subject - " + subject + " Not Found, so create it.");
            } else {
                log.info("Subject - " + subject + "Found.");
            }
        } catch (UnirestException ue) {
            ue.printStackTrace();
        }
    }
}
