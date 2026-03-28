FROM python:3.13-slim
RUN apt-get update && apt-get install -y openjdk-21-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt 
COPY . .

ENV SPARK_HOME=/usr/local/lib/python3.13/site-packages/pyspark
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]