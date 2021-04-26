FROM python:3.8-alpine


WORKDIR /usr/src/app
RUN apk --no-cache add gcc musl-dev
RUN pip install --upgrade pip && pip install --no-cache-dir kafka-python Faker

COPY . .
CMD [ "python", "-u", "./main.py"]
