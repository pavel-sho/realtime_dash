# Use the official Python 3.10 image as the base image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set working directory
WORKDIR /app

# Copy requirements.txt into the container
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . /app/

# Expose the default Streamlit port
EXPOSE 8501

# Set Streamlit configuration to accept connections from outside the container
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Run the Streamlit application
CMD ["sh", "-c", "export DATABRICKS_WAREHOUSE_ID=da42b930da6bc203 && streamlit run app_v5_stav_adjustments.py"]
