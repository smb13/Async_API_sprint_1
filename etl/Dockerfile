FROM python:3.11

ARG USERNAME=app
ARG ROOTDIR=/opt/app

WORKDIR ${ROOTDIR}

RUN groupadd -r ${USERNAME} && \
    useradd -d ${ROOTDIR} -r -g ${USERNAME} ${USERNAME} && \
    mkdir ${ROOTDIR}/.local && \
    mkdir ${ROOTDIR}/logs && \
    mkdir ${ROOTDIR}/state && \
    chown ${USERNAME}:${USERNAME} -R ${ROOTDIR}

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PIP_DISABLE_PIP_VERSION_CHECK 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240

COPY requirements.txt requirements.txt

USER ${USERNAME}

RUN pip install --upgrade pip --no-cache-dir --no-python-version-warning && \
    pip install -r requirements.txt --no-cache-dir

USER 0

COPY *.py ${ROOTDIR}/

USER ${USERNAME}

CMD [ "python", "etl.py" ]