#!/usr/bin/env bash
mvn package -DskipTests; java -jar target/??.jar
