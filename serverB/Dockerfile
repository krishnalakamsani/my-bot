FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y build-essential gcc --no-install-recommends \
	&& pip install --no-cache-dir -r requirements.txt psycopg2-binary redis dhanhq \
	&& apt-get remove -y build-essential gcc \
	&& apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Default command runs the FastAPI backend; execution_api and other services override command in compose
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
