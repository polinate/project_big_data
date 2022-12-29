FROM python:3.10.7

WORKDIR /opt/app

RUN pip install kafka-python psycopg2

COPY ./ . 

CMD ["python3", "consumer.py"]
