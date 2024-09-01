FROM apache/airflow:2.9.3

USER root

# ติดตั้งแพ็คเกจที่จำเป็นและ Java
RUN apt-get update && \
    apt-get install -y procps curl && \
    curl -fsSL https://download.java.net/java/GA/jdk22.0.1/c7ec1332f7bb44aeba2eb341ae18aca4/8/GPL/openjdk-22.0.1_linux-x64_bin.tar.gz \
    | tar -xz -C /usr/local && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ตั้งค่าตัวแปรสภาพแวดล้อม JAVA_HOME
ENV JAVA_HOME=/usr/local/jdk-22.0.1

RUN mkdir -p /opt/airflow/spark/jars
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -o /opt/airflow/spark/jars/gcs-connector-hadoop3-latest.jar || \
    (echo "Failed to download JAR file" && exit 1)

COPY rapid-idiom-432113-m4-8bf13c095a8f.json /opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/spark/rapid-idiom-432113-m4-8bf13c095a8f.json
# เปลี่ยนไปใช้ผู้ใช้ airflow และติดตั้ง Python dependencies
USER airflow
COPY requirements.txt /
RUN pip install numpy && \
    pip install --no-cache-dir -r /requirements.txt
