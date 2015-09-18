[![Build Status](https://travis-ci.org/codepitbull/vertx3-kafka.svg?branch=master)](https://travis-ci.org/codepitbull/vertx3-kafka)

This is a little play-project of mine to test out integration if Kafka with Vert.x. 

It's by no means complete and lacks in several ares (especially the handling of multiple consumers).

# Build
./gradlew clean build publishToMavenLocal

# Vert.x 3 Kafka Module #
This module is based on Vert.x 3 and Kafka 0.8.2.

# KafkaHighLevelConsumerVerticle #
A consumer based on the 
[High Level Consumer API from Kafka 0.8.2](http://kafka.apache.org/documentation.html#highlevelconsumerapi).

# KafkaSimpleConsumerVerticle #
A consumer based on the 
[Simple Consumer API from Kafka 0.8.2](http://kafka.apache.org/documentation.html#simpleconsumerapi). The main 
difference is that one can adjust the reading offset. For now it is only possible to read and not to commit. There
are still a couple of problems with that part of the API, which make commiting using the Java-API impossible.

I will update this module as soon as the problems get fixed (or they update their documentation).

# On Producers #
The producer API is now async by default and easy to use. If you need an example on how to use it in Vert.x take a 
look at my _KafkaProducerVerticle_ I use for the tests.

# TEST RUNTIME #
I am using an embedded Kafka instance for testing.

__TESTS ARE VERY HEAVYWEIGHT__

# Get Kafka #
I use Docker with the following image for my dev-setup: 
 
docker pull devdb/kafka:latest
 
docker run -d --name kafka -p 2181:2181 -p 9092:9092 devdb/kafka

don't forget to add the docker-name to /etc/hosts as otherwise the conection will fail!


