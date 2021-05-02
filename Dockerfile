FROM bde2020/spark-submit:2.4.0-hadoop2.7

MAINTAINER Daniel Marchena <danielmapar@gmail.com>

ARG SBT_VERSION
ENV SBT_VERSION=${SBT_VERSION:-1.2.8}

RUN wget -O - https://piccolo.link/sbt-1.2.8.tgz | gunzip | tar -x -C /usr/local

ENV PATH /usr/local/sbt/bin:${PATH}

WORKDIR /app

# Pre-install base libraries
ADD build.sbt /app/
ADD project/plugins.sbt /app/project/
RUN sbt update

COPY template.sh /

ENV SPARK_APPLICATION_MAIN_CLASS ClassificationJob

# Copy the source code and build the application
COPY . /app
RUN sbt assembly

# ~reStart run
CMD ["./template.sh"]