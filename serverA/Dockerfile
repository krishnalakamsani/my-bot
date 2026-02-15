FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir dhanhq redis psycopg2-binary

CMD ["python", "feed_service.py"]
