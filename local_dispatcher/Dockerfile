FROM python:3.7.2-slim
LABEL python_version=python3.7.2

RUN printf "deb http://mirrors.163.com/debian/ stretch main non-free contrib\ndeb http://mirrors.163.com/debian/ stretch-updates main non-free contrib\ndeb http://mirrors.163.com/debian/ stretch-backports main non-free contrib\ndeb-src http://mirrors.163.com/debian/ stretch main non-free contrib\ndeb-src http://mirrors.163.com/debian/ stretch-updates main non-free contrib\ndeb-src http://mirrors.163.com/debian/ stretch-backports main non-free contrib\ndeb http://mirrors.163.com/debian-security/ stretch/updates main non-free contrib\ndeb-src http://mirrors.163.com/debian-security/ stretch/updates main non-free contrib" > /etc/apt/sources.list

# Set virtualenv environment variables. This is equivalent to running
# source /env/bin/activate
RUN apt-get update -y
RUN apt-get install -y python3-dev
ADD local_dispatcher/requirements.txt /app/
RUN pip3 install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip3 install -r /app/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

ADD local_dispatcher/ /app/
ADD shared/ /app/
WORKDIR /app/
#CMD exec gunicorn --bind :$PORT --timeout 120 --worker-class eventlet --workers 3 --no-sendfile --config python:my_conf main:app
EXPOSE 8085
CMD python local_dispatch.py