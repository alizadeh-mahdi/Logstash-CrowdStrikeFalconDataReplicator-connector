import data_replicator_config
import json
import os
import time
import zlib
import socket
from io import BytesIO
import gzip
import StringIO
import logging
from logging.handlers import RotatingFileHandler


try:
    import boto3
except ImportError as err:
    app_log.info(err)
    app_log.info('boto3 is required to run data_replicator_sample_consumer.  Please "pip install boto3"!')

############################################Log Settings########################################
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')

logFile = '/var/log/crowdstrike/falcon_data_replicator.log'

my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024,
                                 backupCount=2, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)

app_log = logging.getLogger('root')
app_log.setLevel(logging.INFO)

app_log.addHandler(my_handler)


###################################################################################################
# NOTE: See Falcon Data Replicator instructions for details on how  to use this sample consumer.  #
###################################################################################################

AWS_KEY = data_replicator_config.AWS_KEY
AWS_SECRET = data_replicator_config.AWS_SECRET
QUEUE_URL = data_replicator_config.QUEUE_URL
OUTPUT_PATH = data_replicator_config.OUTPUT_PATH
VISIBILITY_TIMEOUT = data_replicator_config.VISIBILITY_TIMEOUT

sqs = boto3.resource('sqs', region_name='us-west-1', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
s3 = boto3.client('s3', region_name='us-west-1', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
queue = sqs.Queue(url=QUEUE_URL)

host = socket.gethostname()
port = 11199                   # The same port as used by the server
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    app_log.info('connected to logstash: first attempt')
except:
    app_log.info('connection to logstash was refused')


def forward_to_logstash(json_content):
    status = 0
    global s
    while(status == 0):
       try:
          s.sendall(json.dumps(json_content))
          s.send('\n')
          #print 'file sent' + json.dumps(json_content)
          status = 1
       except Exception as e:           
          app_log.info(e)       
          s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          s.connect((host, port))
          app_log.info( 'new attempt to create a connection with logstash')
          time.sleep(10)

def get_table_name(file_path):
    table = ""
    file_path = str(file_path)
    if file_path.startswith('data'):
      table = "data"
    elif("appinfo" in file_path):
      table = "appinfo"
    elif("userinfo" in file_path):
      table = "userinfo"
    elif("aidmaster" in file_path):
      table = "aid_master"
    elif("managedassets" in file_path):
      table = "managedassets"
    elif("notmanaged" in file_path):
      table = "notmanaged"
    return table

def download_message_files(msg):
    for s3_file in msg['files']:
        s3_path = s3_file['path']
        app_log.info('Downloaded file to path %s' % s3_path)
        obj = s3.get_object(Bucket=msg['bucket'], Key=s3_path)                        
        n = obj.get('Body').read()
        gzipfile = BytesIO(n)
        gzipfile = gzip.GzipFile(fileobj=gzipfile)
        content = gzipfile.read()
        buf = StringIO.StringIO(content)
        table_name = get_table_name(s3_path)
        for line in buf: 
           data = json.loads(line)      
           data.update({'table': table_name})
           forward_to_logstash(data)


def consume_data_replicator():
    """Consume from data replicator and track number of messages/files/bytes downloaded."""

    sleep_time = 1
    msg_cnt = 0
    file_cnt = 0
    byte_cnt = 0

    while True:  # We want to continuously poll the queue for new messages.
        # Receive messages from queue if any exist (NOTE: receive_messages() only receives a few messages at a
        # time, it does NOT exhaust the queue)
        for msg in queue.receive_messages(VisibilityTimeout=VISIBILITY_TIMEOUT):
            msg_cnt += 1
            body = json.loads(msg.body)  # grab the actual message body
            download_message_files(body)
            file_cnt += body['fileCount']
            byte_cnt += body['totalSize']
            # msg.delete() must be called or the message will be returned to the SQS queue after
            # VISIBILITY_TIMEOUT seconds
            msg.delete()
            time.sleep(sleep_time)

        #print "Messages consumed: %i\tFile count: %i\tByte count: %i" % (msg_cnt, file_cnt, byte_cnt)

if __name__ == '__main__':
    consume_data_replicator()
