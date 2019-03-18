FROM python:3.6

# Avoid buffering stdout.
ENV PYTHONUNBUFFERED 1

RUN mkdir /code
WORKDIR /code

COPY . /code/
RUN pip install -r requirements.txt
RUN ["chmod", "+x", "./startup"]

CMD ["./startup"]
