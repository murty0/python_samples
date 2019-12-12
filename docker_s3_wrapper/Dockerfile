FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y rsyslog cron nano python3-pip
RUN pip3 install --upgrade pip
RUN pip install boto3
RUN pip install supervisor

RUN echo "supervisord -c /etc/supervisord.conf" >> /root/.bashrc

RUN touch /var/log/cron.log
RUN mkdir /s3_wrapper
WORKDIR /s3_wrapper
ADD . /s3_wrapper/

COPY s3-cron /etc/cron.d/s3-cron
COPY supervisord.conf /etc/supervisord.conf
RUN chmod 0644 /etc/cron.d/s3-cron
RUN chmod u+x /s3_wrapper/s3_wrapper.py
RUN chmod u+x /s3_wrapper/s3_wrapper_user.py
RUN crontab /etc/cron.d/s3-cron
ENV PYTHONUNBUFFERED 1

#uncomment below 'CMD' and comment the "ENTRYPOINT" if you do not want the logs to hold up the terminal.
#CMD ["/bin/bash", "-c", "(cron) && :>> /var/log/cron.log && tail -f /var/log/cron.log"]

ENTRYPOINT ["/bin/bash", "-c", "(cron) && :>> /var/log/cron.log && tail -f /var/log/cron.log"]
