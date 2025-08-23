FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip3 install requirements.txt
COPY producer.py .
CMD ["python3", "producer.py"]