FROM python:3.7.2-slim
LABEL python_version=python3.7.2

RUN printf "deb http://mirrors.163.com/debian/ stretch main non-free contrib\ndeb http://mirrors.163.com/debian/ stretch-updates main non-free contrib\ndeb http://mirrors.163.com/debian/ stretch-backports main non-free contrib\ndeb-src http://mirrors.163.com/debian/ stretch main non-free contrib\ndeb-src http://mirrors.163.com/debian/ stretch-updates main non-free contrib\ndeb-src http://mirrors.163.com/debian/ stretch-backports main non-free contrib\ndeb http://mirrors.163.com/debian-security/ stretch/updates main non-free contrib\ndeb-src http://mirrors.163.com/debian-security/ stretch/updates main non-free contrib" > /etc/apt/sources.list

# Set virtualenv environment variables. This is equivalent to running
# source /env/bin/activate
RUN seq 1 8 | xargs -I{} mkdir -p /usr/share/man/man{}
RUN for i in {1..8}; do mkdir -p "/usr/share/man/man$i"; done
RUN apt-get update
RUN apt-get install -y libpq-dev python3-dev build-essential postgresql postgresql-contrib
ADD default/requirements.txt /app/
RUN pip3 install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip3 install -r /app/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

ADD default/ /app/
ADD shared /app/shared

WORKDIR /app/
RUN ["chmod", "+x", "/app/db-init.sh"]
#CMD exec gunicorn --bind :$PORT --timeout 120 --worker-class eventlet --workers 3 --no-sendfile --config python:my_conf main:app
EXPOSE 8080
CMD exec gunicorn --bind :8080 --timeout 120 --worker-class sync --workers 5 --no-sendfile main:app