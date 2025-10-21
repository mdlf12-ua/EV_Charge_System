# 1. Usa una imagen base oficial de Python
FROM python:3.12-slim

WORKDIR /app


COPY requirements.txt .
COPY EV_Central.py .


RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "EV_Central.py"]
