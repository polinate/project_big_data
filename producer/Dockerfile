FROM python:3.10.7

WORKDIR /opt/app

RUN pip install requests kafka-python

COPY ./ .

CMD ["python3", "producer.py"]
