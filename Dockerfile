FROM openjdk:11

COPY . /incubator-wayang

WORKDIR /incubator-wayang

RUN ./mvnw clean install \
  -DskipTests \
  -Drat.skip=true # skip checks for licensed files because Dockerfile isn't approved

RUN ./mvnw clean package -pl :wayang-assembly -Pdistribution

RUN tar -xvf wayang-assembly/target/apache-wayang-assembly-0.7.1-incubating-dist.tar.gz

ENV WAYANG_HOME=/incubator-wayang/wayang-0.7.1
ENV PATH=$PATH:$WAYANG_HOME/bin

# TODO: Set up Spark and other dependencies
