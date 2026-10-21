#use lightweight python image
FROM python:3.11-slim

#set working directory inside the container
WORKDIR /app

#install required google api packages 
RUN pip install --no-cache-dir google-api-python-client google-auth

#copy them migration script into the container
COPY gdrive_migrate_pure.py .

#Set the script as a entrypoint 
ENTRYPOINT ["python", "gdrive_migrate_pure.py"]