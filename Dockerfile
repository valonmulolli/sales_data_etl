# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

# Create directories for data and logs
RUN mkdir -p /app/data/input /app/data/output /app/data/archive /app/logs

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app:${PATH}"

# Make port 8000 available to the world outside this container (if you plan to add API later)
EXPOSE 8000

# Define volume for persistent data and logs
VOLUME ["/app/data", "/app/logs"]

# Run the ETL pipeline when the container launches
CMD ["python", "main.py"]
