FROM python:3.6

# Avoid buffering stdout.
ENV PYTHONUNBUFFERED 1

RUN mkdir /code
WORKDIR /code

ADD requirements.txt /code/
RUN pip install -r requirements.txt
COPY . /code/

CMD ["./startup"]