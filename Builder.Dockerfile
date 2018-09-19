FROM maven:alpine

# speed up Maven JVM a bit
ENV MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"

WORKDIR /usr/src/app/

# install maven dependency packages (keep in image)
COPY pom.xml /usr/src/app/
COPY checkstyle*.xml /usr/src/app/
RUN mvn -T 1C install && rm -rf target
