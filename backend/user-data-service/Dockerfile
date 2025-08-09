# backend/session-service/Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt requirements.txt
COPY proto/ ./proto/
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "app.py"]
