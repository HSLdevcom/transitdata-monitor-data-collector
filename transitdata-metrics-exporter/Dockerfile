FROM hsldevcom/infodevops-docker-base-images:1.0.0-java-25
COPY target/transitdata-metrics-exporter.jar /usr/app/transitdata-metrics-exporter.jar

ENTRYPOINT ["java", "-XX:InitialRAMPercentage=70", "-XX:MaxRAMPercentage=70", "-jar", "/usr/app/transitdata-metrics-exporter.jar"]
