## The demo project for Apache SkyWalking and Apache Pulsar integration

### Run the demo program with following steps

1. Clone the repo
2. Run `mvn clean install` to build the fat jar
3. Start a Pulsar standalone
4. Start SkyWalking service and UI
5. Run the demo program with agent ` java -javaagent:/{skywalking_home}/agent/skywalking-agent.jar -DSW_AGENT_NAME=demo-application -jar skywalking-pulsar-demo.jar`
6. Run `curl http://localhost:8088/demo/data-generate` to generate data
7. Check the Pulsar tracing data at `http://localhost:8080/trace`