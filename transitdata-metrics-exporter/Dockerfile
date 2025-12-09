FROM eclipse-temurin:25-alpine
COPY target/transitdata-metrics-exporter.jar /usr/app/transitdata-metrics-exporter.jar

ENTRYPOINT ["java", "-XX:InitialRAMPercentage=70", "-XX:MaxRAMPercentage=70", "-jar", "/usr/app/transitdata-metrics-exporter.jar"]
