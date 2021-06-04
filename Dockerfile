FROM python:3
WORKDIR /app
ADD pyproject.toml .
RUN pip install poetry && poetry install
ADD . .
ENV TZ=EST
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
CMD poetry run python server.py