FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir more-itertools==9.0.0
CMD ["python3", "-m", "aue_rues.etl"]


# Docker commands to create image and run container:
# cd aue_rues
# docker build -t aue_rues .
# cd ..
# docker run -it --rm -v /mnt/OGD-DataExch/AUE-RUES/korrigiert:/code/data-processing/aue_rues/data_orig -v /data/dev/workspace/data-processing:/code/data-processing --name aue_rues aue_rues

# Docker on Mac:
# docker run -it --rm -v /mnt/OGD-DataExch/AUE-RUES/korrigiert:/code/data-processing/aue_rues/data_orig -v /Users/jonasbieri/PycharmProjects/data-processing:/code/data-processing --name aue_rues aue_rues

# For Docker on windows running with Linux containers: Run in (git) bash, use env var HTTPS_PROXY of bash as build-arg (see https://vsupalov.com/docker-build-pass-environment-variables):
# cd aue_rues
# docker build -t aue_rues --build-arg HTTPS_PROXY=${HTTPS_PROXY} .
# cd ..
# winpty docker run -it --rm --env HTTPS_PROXY=${HTTPS_PROXY} -v "C:\dev\workspace\data-processing":/code/data-processing --name aue_rues aue_rues


