FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .


RUN mkdir -p /var/log/gtfs2kafka
CMD ["python3", "gtfs2kafka.py"]