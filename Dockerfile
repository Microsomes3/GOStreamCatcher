FROM arm64v8/ubuntu:latest

RUN apt-get update -y

RUN apt-get install curl -y

RUN apt-get update -y

RUN apt-get install -y golang

RUN apt-get install build-essential -y

RUN apt-get update -y

RUN apt-get install ffmpeg -y

RUN apt-get install wget -y

RUN apt-get install -y python3-pip

RUN pip install yt-dlp

COPY . .

RUN go mod download
RUN go mod verify
RUN go build -o mathapp

RUN apt-get update && apt-get install -y libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2
RUN apt-get install -y libx11-xcb1 libxcomposite1 libxcursor1 libxdamage1 libxi6 libxtst6 libnss3 libcups2 libxss1 libxrandr2 libgconf-2-4 libasound2 libatk1.0-0 libgtk-3-0


RUN apt-get update && \
    apt-get install -y xvfb && \
    rm -rf /var/lib/apt/lists/*

ENV DISPLAY=:99

CMD ["./mathapp"]