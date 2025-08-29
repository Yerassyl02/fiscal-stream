FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY producer.py .
CMD ["python3", "-u", "producer.py"]