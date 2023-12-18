FROM python:3.11

ARG USERNAME=app
ARG ROOTDIR=/opt/app

WORKDIR ${ROOTDIR}

RUN groupadd -r ${USERNAME} && \
    useradd -d ${ROOTDIR} -r -g ${USERNAME} ${USERNAME} && \
    mkdir ${ROOTDIR}/.local && \
    mkdir ${ROOTDIR}/logs && \
    chown ${USERNAME}:${USERNAME} -R ${ROOTDIR}


ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PIP_DISABLE_PIP_VERSION_CHECK 1

ENV PATH ${ROOTDIR}/.local/bin:$PATH

COPY requirements.txt requirements.txt

USER ${USERNAME}

RUN pip install --upgrade pip --no-cache-dir --no-python-version-warning && \
    pip install -r requirements.txt --no-cache-dir

USER 0

COPY api ${ROOTDIR}/api
COPY core ${ROOTDIR}/core
COPY db ${ROOTDIR}/db
COPY models ${ROOTDIR}/models
COPY services ${ROOTDIR}/services
COPY main.py ${ROOTDIR}/
COPY gunicorn.conf.py ${ROOTDIR}/

USER ${USERNAME}

EXPOSE 8000

CMD [ "gunicorn", "main:app" ]