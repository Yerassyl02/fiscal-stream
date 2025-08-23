FROM python:3.12-slim
WORKDIR /app
COPY main.py products.json ./
RUN pip3 install fastapi uvicorn sse-starlette Faker
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]