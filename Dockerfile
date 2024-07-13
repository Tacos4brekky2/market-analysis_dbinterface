FROM python:3.12-alpine
ENV FLASK_ENV=production
ENV FLASK_DEBUG=0
WORKDIR /app
COPY app /app
COPY requirements.txt /app
RUN apk update && apk add --no-cache gcc musl-dev libffi-dev
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python3", "app.py"]
