# syntax=Dockerfile:1
FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
USER root

COPY read_open_aq_json.py .
# WORKDIR /app
# RUN pip install --user
RUN pip install pyspark==3.1.2
RUN pip install boto3

CMD ["python3", "read_open_aq_json.py"]