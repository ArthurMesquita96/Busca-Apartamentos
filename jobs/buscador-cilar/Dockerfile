FROM python:3.11-slim

ENV PYTHONUNBUFFEDRED True

ENV APP_HOME /app
ENV PORT 8080
WORKDIR $APP_HOME
COPY . ./


# Install manually all the missing libraries
RUN apt-get update
RUN apt-get install -y wget gconf-service libasound2 libatk1.0-0 libcairo2 libcups2 libfontconfig1 libgdk-pixbuf2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libxss1 fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils

# Install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install


RUN pip install --no-cache-dir -r requirements.txt 

CMD ["python", "main.py"]
