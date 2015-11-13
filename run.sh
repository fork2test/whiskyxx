#!/usr/bin/env bash
mvn package
java -jar target/whisky-1.0-SNAPSHOT-fat.jar \
  --config="src/main/conf/my-application-conf.json" \
  --redeploy="src/**/*.js,src/**/*.java,src/**/*.html,src/**/*.jade" \
  --onRedeploy="./run.sh"