# kafka-connect-UKMEETOFFICE
This package offers a Kafka Connect to fetch meteorological data from UK Met Office.
The connect monitor read message from AWS SOS to get new file notice. Then download the proper file in NetCDF format from S3.
Once download, data is parsed and sent interested parts to Kafka
For now, we only support air pressure data subject for demo purpose.

### Features TODO
- [x] Support message purge
- [ ] Support more data subject
- [ ] Support consuming more messages at one poll

### Config Values ###
* __topic__ - The topic to publish data to
* __interval__ - How often to check for new file(s) to be processed, __(default : 10 seconds)__
* __filename__ - Match the data file to read
* __schema.registry.uri__ - The URI to the Schema Registry  
* __schema.subject__ - The subject used to validate avro schema __(default : topic)__
