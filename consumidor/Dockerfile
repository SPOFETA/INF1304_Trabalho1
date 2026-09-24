FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

# -u: saída sem buffer, para os logs aparecerem em tempo real no docker logs
CMD ["python", "-u", "consumer.py"]
