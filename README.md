# Meditation VR - Spark ML Job

This repository contains a Spark Streaming + Spark ML job that consumes data from a Kafka Queue (EEG topic) and generates ML models on the fly. Also, this repo has a second Spark job responsible for consuming Kafka and classifying brainwaves in real-time with previously generated ML models.

## Getting Started

* To run this api just follow the steps on [compose](https://github.com/MeditationVR/compose)