# sp3_psutil

Publish system metrics over MQTT and Sparkplug 3

This is a little project to learn sparkplug 3 and to maybe help others get started.

## Features
* Acts as a sparkplug 3 node - sends birth and death messages
* Sends the following metrics periodically: 'System/CpuPercent', 'System/DiskUsage'
* Sends once in birth message: 'Properties/OS' and 'Properties/OS Version'
* Supports command 'Node Control/Rebirth' to trigger a resending of birth message
* Gracefully reconnects to MQTT broker

## Installation
Install Python 3
To install dependencies, run command 
> pip install -r requirements.txt
> 

## Usage
> usage: sp3_psutil.py [-h] [--host HOST] [--port PORT] [--username USERNAME] [--password PASSWORD] [--interval INTERVAL] [--group_id GROUP_ID] [--node_id NODE_ID]  
>  
> options:
>  -h, --help           show this help message and exit  
>  --host HOST          MQTT broker host  
>  --port PORT          MQTT broker port  
>  --username USERNAME  MQTT username  
>  --password PASSWORD  MQTT password  
>  --interval INTERVAL  Send interval [seconds]  
>  --group_id GROUP_ID  SP3 group id  
>  --node_id NODE_ID    SP3 node id  

## Example
> python sp3_psutil --host localhost --port 1883 --username user --password pass --interval 2 --group_id sparkplug --node-id mynode 