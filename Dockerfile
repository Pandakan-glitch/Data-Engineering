FROM apache/airflow:2.9.2

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install Python packages (RUN as airflow user)
RUN pip install --no-cache-dir \
    dateparser \
    pandas \
    SQLAlchemy \
    word2number \
    pyspark \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-spark==5.0.0