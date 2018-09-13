FROM openjdk:8-jre

RUN apt-get update && apt-get install -y postgresql-client
COPY ./wait-for-postgres.sh /usr/local/bin/wait-for-postgres.sh

COPY target/lib /usr/share/bigtable-autoscaler/lib
COPY target/bigtable-autoscaler.jar /usr/share/bigtable-autoscaler/bigtable-autoscaler.jar

CMD ["/usr/bin/java", "-cp", "/usr/share/bigtable-autoscaler/bigtable-autoscaler.jar:/usr/share/bigtable-autoscaler/lib/*", "com.spotify.autoscaler.Main"]
