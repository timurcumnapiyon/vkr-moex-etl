FROM prefecthq/prefect:3.6.10-python3.11
COPY requirements.txt /opt/prefect/vkr/requirements.txt
RUN uv pip install -r /opt/prefect/vkr/requirements.txt && \
    python -m pip install --no-cache-dir prefect-docker==0.7.1
COPY . /opt/prefect/vkr/
WORKDIR /opt/prefect/vkr/
