# Vert.x simple reactive example

Example project for creating a Vert.x module which contains a consumer and a sink. You can simulate a failure in the sink so the consumer stops consuming.  

Build and run:

`mvn clean install && vertx runzip target/vertx2-reactive-1.0-SNAPSHOT-mod.zip`

* Disable sink: 

`curl -v localhost:8080/sink/off`
* Enable  sink: 

`curl -v localhost:8080/sink/on`
