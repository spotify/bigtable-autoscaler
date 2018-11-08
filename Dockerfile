FROM openjdk:11-jre

ENTRYPOINT [ "/usr/bin/java", "-cp", "/usr/share/bigtable-autoscaler/bigtable-autoscaler.jar:/usr/share/bigtable-autoscaler/lib/*", "com.spotify.autoscaler.Main"]

ADD target/lib /usr/share/bigtable-autoscaler/lib
ADD target/bigtable-autoscaler.jar /usr/share/bigtable-autoscaler/bigtable-autoscaler.jar
