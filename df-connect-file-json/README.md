# kafka-connect-json
Reads JSON files from the filesystem (based on a glob), validates against Confluent schema registry and if valid, converts to Avro and publishes on topic.

### Install and Run Dependencies ###
* Java - http://www.oracle.com/technetwork/java/javase/downloads/index.html (Java 7 or Higher)
* Gradle - https://docs.gradle.org/current/userguide/installation.html
* Confluent Platform  
    Binaries - http://docs.confluent.io/3.0.0/installation.html#installation  
    Source - https://github.com/confluentinc/kafka **AND** https://github.com/confluentinc/schema-registry  
* Start Zookeeper, Kafka, and Schema Registry
* Post schema(s) to Schema Registry

### Get Started ###
    git clone https://github.com/notchco/kafka-connect-json  
    
    cd kafka-connect-json && gradle wrapper
    
    ./gradlew installWtihDeps
    
    CLASSPATH=`pwd`/build/libs/* /path/to/kafka/bin/connect-standalone.sh connect-standalone.properties connect-json-source.properties 

### Use Custom Dependencies ###
* Make sure those dependencies are in the local maven repo
* Uncomment this line in build.gradle
    * //	mavenLocal()

### Config Values ###
* __topic__ - The topic to publish data to  
* __file.location__ - The location of the file(s) to process  
* __file.glob__ - The glob criteria  
* file.glob.interval - How often to check for new file(s) to be processed __(default : 10 seconds)__
* file.overwrite - If a file is modified should it be republished to kafka __(default : false)__  
* __schema.registry.uri__ - The URI to the Schema Registry  
* schema.subject - The subject used to validate avro schema __(default : topic+"-value")__  
* schema.version - The version of the subject to be used for schema validation __(default : latest version of the subject)__  
