FROM apache/sedona:1.6.0

WORKDIR /app

RUN mkdir /app/data_zip && \
    mkdir /app/data_xml && \
    mkdir /app/data_geoparquet && \
    mkdir /app/data_export

COPY *.bash /app
COPY *.py /app

RUN python3 first_time_local_pyspark_setup.py

ENTRYPOINT [ "/bin/bash", "script_runner.bash" ]
