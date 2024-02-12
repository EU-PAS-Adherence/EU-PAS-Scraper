FROM python:3.8.18-slim-bookworm
LABEL maintainer="Pedram Ramezani" \
      description="Runs the whole extraction and analysis pipeline"

WORKDIR /usr/src/eupas
COPY requirements.txt ./
COPY requirements.additional.txt ./
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt -r requirements.additional.txt

COPY . .
RUN pip install --no-cache-dir -e .

RUN chmod 755 pipeline/run.sh
ENTRYPOINT []
CMD [ "./pipeline/run.sh"]