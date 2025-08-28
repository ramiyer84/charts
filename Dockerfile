FROM eclipse-temurin:21.0.7_6-jre AS base

ARG http_proxy=http://uaclplgapp27:8080
ARG no_proxy=localhost,al.intraxa
ENV HTTP_PROXY=${http_proxy} \
    HTTPS_PROXY=${http_proxy} \
    NO_PROXY=${no_proxy}

# Add corporate CAs (tolerate 200 or 304 revalidation)
ADD http://uaclplgapp23.al.intraxa/dl/AXA-PROXY-ROOT-CA.crt  /usr/local/share/ca-certificates/AXA-PROXY-ROOT-CA.crt
ADD http://uaclplgapp23.al.intraxa/dl/katello-server-ca.pem  /usr/local/share/ca-certificates/katello-server-ca.pem

RUN set -eux; \
    mkdir /app; \
    update-ca-certificates; \
    openssl x509 -in /usr/local/share/ca-certificates/AXA-PROXY-ROOT-CA.crt -inform pem -out /tmp/AXA-PROXY-ROOT-CA.der -outform der; \
    openssl x509 -in /usr/local/share/ca-certificates/katello-server-ca.pem -inform pem -out /tmp/katello-server-ca.der -outform der; \
    keytool -importcert -alias AXA-PROXY-ROOT-CA -keystore /opt/java/openjdk/lib/security/cacerts -storepass changeit -noprompt -file /tmp/AXA-PROXY-ROOT-CA.der; \
    keytool -importcert -alias katello-server-ca -keystore /opt/java/openjdk/lib/security/cacerts -storepass changeit -noprompt -file /tmp/katello-server-ca.der; \
    groupadd -g 1001 devops; \
    useradd -u 1001 -g 1001 devops; \
    mkdir -p /app/config; \
    chown -R devops /app

FROM base AS app
ARG artifact_version="2.1.0"

COPY build/libs/dms-migration-service-${artifact_version}.jar /app/dms-migration-service-${artifact_version}.jar
ADD https://appsstaging.al.intraxa/dl/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar

RUN chmod 644 /app/opentelemetry-javaagent.jar && \
    chown 1001:1001 /app/opentelemetry-javaagent.jar && \
    ln -s /app/dms-migration-service-${artifact_version}.jar /app/dms-migration-service.jar && \
    mkdir -p /app/config && chown -R devops /app

EXPOSE 8080 5005
WORKDIR /app
USER devops

# Debug target – use shell form so $JAVA_OPTS expands
FROM app AS debug
CMD sh -c 'java $JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -jar /app/dms-migration-service.jar'

# Default target – shell form so $JAVA_OPTS expands
FROM app
CMD sh -c "java $JAVA_OPTS -jar /app/dms-migration-service.jar"