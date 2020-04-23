# Logstash-CrowdStrikeFalconDataReplicator-connector
Use this code to poll CrowdStrike logs and forward it to Logstash
In the following link, you can find the instructions for using CrowdStrike FDR:
https://falcon.crowdstrike.com/support/documentation/9/falcon-data-replicator

In this code, I have extended the original python code provided by CS in order to forward the collected events to Logstash via TCP:
In conf.d folder you can see how you can set the settings in Logstash in order to open a TCP port.
