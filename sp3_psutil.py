import argparse
import socket  # Only used for getting host name of current machine
import time
import psutil
import paho.mqtt.client as mqtt
import sparkplug_b_pb2 as spb  # The python code generated from protoc
import platform

# Datatype IDs taken from sparkplug_b.proto
DATATYPE_UINT32 = 7
DATATYPE_FLOAT = 9
DATATYPE_BOOLEAN = 11
DATATYPE_STRING = 12

# Aliases for the used metrics
ALIAS_BDSEQ = 1
ALIAS_CPU_PERCENT = 2
ALIAS_DISK_USAGE = 3
ALIAS_REBIRTH = 4
ALIAS_OS = 5
ALIAS_OS_VERSION = 6

SEQ = 0  # Sequence number of payload
BDSEQ = 0  # Birth death sequence number
IS_CONNECTED = False
LAST_USAGE_PERCENT: int | None = None

# Name strings for the metrics
METRIC_NAME_REBIRTH = "Node Control/Rebirth"
METRIC_NAME_BDSEQ = "bdSeq"
METRIC_NAME_DISK_USAGE = "System/DiskUsage"
METRIC_NAME_CPU_PERCENT = "System/CpuPercent"


# Bundle group_id and node_id into a single object
class Identifier:
    def __init__(self, group_id: str, node_id: str):
        self.group_id = group_id
        self.node_id = node_id


def now_millis() -> int:
    return int(time.time() * 1000)


def next_seq() -> int:
    # Count up from 0 to 255, then start at 0 again
    global SEQ
    seq = SEQ
    if SEQ == 255:
        SEQ = 0
    else:
        SEQ += 1
    return seq


def add_metric(payload: spb.Payload, datatype: int, alias: int) -> spb.Payload.Metric:
    metric = payload.metrics.add()
    metric.alias = alias
    metric.datatype = datatype
    metric.timestamp = now_millis()
    return metric


def add_uint32_metric(
    payload: spb.Payload, alias: int, value: int
) -> spb.Payload.Metric:
    metric = add_metric(payload, DATATYPE_UINT32, alias)
    metric.int_value = value
    return metric


def add_float_metric(
    payload: spb.Payload, alias: int, value: float
) -> spb.Payload.Metric:
    metric = add_metric(payload, DATATYPE_FLOAT, alias)
    metric.float_value = value
    return metric


def create_payload(is_birth: bool) -> spb.Payload:
    now = now_millis()

    payload = spb.Payload()
    payload.timestamp = now
    payload.seq = next_seq()

    # Fetch values from psutil and write to metrics
    usage = psutil.disk_usage("/").percent
    global LAST_USAGE_PERCENT
    if usage != LAST_USAGE_PERCENT:
        # Send disk usage only if it actually changed
        LAST_USAGE_PERCENT = usage
        disk_usage = add_float_metric(payload, ALIAS_DISK_USAGE, usage)
    else:
        disk_usage = None

    cpu_percent = add_float_metric(
        payload, ALIAS_CPU_PERCENT, psutil.cpu_percent(interval=1)
    )

    if is_birth:
        # Only on birth message:
        # * send metrics with name strings
        # * Send bdseq value
        # * Send metrics defining commands

        # Define bdSeq value
        bdseq = add_uint32_metric(payload, ALIAS_BDSEQ, BDSEQ)

        # Set name strings
        bdseq.name = METRIC_NAME_BDSEQ

        disk_usage.name = METRIC_NAME_DISK_USAGE
        cpu_percent.name = METRIC_NAME_CPU_PERCENT

        # Define rebirth metric. Must not have an alias defined!
        rebirth = payload.metrics.add()
        rebirth.datatype = DATATYPE_BOOLEAN
        rebirth.timestamp = now
        rebirth.name = METRIC_NAME_REBIRTH
        rebirth.boolean_value = False

        os = payload.metrics.add()
        os.datatype = DATATYPE_STRING
        os.timestamp = now
        os.name = "Properties/OS"
        os.alias = ALIAS_OS
        os.string_value = platform.system()

        version = payload.metrics.add()
        version.datatype = DATATYPE_STRING
        version.timestamp = now
        version.name = "Properties/OS Version"
        version.alias = ALIAS_OS_VERSION
        version.string_value = platform.version()

    return payload


def on_connect(client: mqtt.Client, ident: Identifier, flags, rc):
    print(
        f"Connected with group ID '{ident.group_id}', node ID {ident.node_id}, result code "
        + str(rc)
    )
    global IS_CONNECTED
    IS_CONNECTED = True

    cmd_topic = f"spBv1.0/{ident.group_id}/NCMD/{ident.node_id}"
    client.subscribe(cmd_topic)

    send_birth(client, ident)

    global BDSEQ
    BDSEQ += 1


def on_disconnect(client: mqtt.Client, ident: Identifier, rc):
    print("Disconnected with return code: " + str(rc))
    global IS_CONNECTED
    IS_CONNECTED = False

    while True:
        try:
            print("Trying to reconnect to the broker...")
            send_death_message(client, ident)
            client.reconnect()
            break
        except Exception as e:
            print("Failed to reconnect to the broker:", e)


def on_message(client: mqtt.Client, ident: Identifier, message: mqtt.MQTTMessage):
    payload = spb.Payload()
    payload.ParseFromString(message.payload)

    for metric in payload.metrics:
        print(f"Received cmd metric with name '{metric.name}', alias {metric.alias}")
        if metric.name == METRIC_NAME_REBIRTH or metric.alias == ALIAS_REBIRTH:
            birth_topic = f"spBv1.0/{ident.group_id}/NBIRTH/{ident.node_id}"

            print(
                f"Received rebirth command. Resending NBIRTH to topic '{birth_topic}."
            )
            birth_payload = create_payload(is_birth=True)
            birth_payload_bytes = bytearray(birth_payload.SerializeToString())
            client.publish(birth_topic, birth_payload_bytes)


def send_death_message(client: mqtt.Client, ident: Identifier) -> None:
    topic = f"spBv1.0/{ident.group_id}/NDEATH/{ident.node_id}"
    payload = spb.Payload()
    bdseq = add_uint32_metric(payload, ALIAS_BDSEQ, BDSEQ)
    bdseq.name = "bdSeq"
    client.will_set(
        topic=topic,
        payload=bytearray(payload.SerializeToString()),
        qos=0,
        retain=False,
    )
    print(f"Setting death message to topic '{topic} with bdSeq={BDSEQ}.")


def send_birth(client: mqtt.Client, ident: Identifier) -> None:
    topic = f"spBv1.0/{ident.group_id}/NBIRTH/{ident.node_id}"
    payload = create_payload(is_birth=True)
    client.publish(topic, bytearray(payload.SerializeToString()))
    print(f"Sent birth message to topic '{topic} with bdSeq={BDSEQ}.")


def send_data(client: mqtt.Client, ident: Identifier) -> None:
    topic = f"spBv1.0/{ident.group_id}/NDATA/{ident.node_id}"
    payload = create_payload(is_birth=False)
    client.publish(topic, bytearray(payload.SerializeToString()))
    print(f"Sent data message to topic '{topic}.")


def run(
    host: str,
    port: int,
    username: str,
    password: str,
    interval: int,
    ident: Identifier,
) -> None:
    # Connect to MQTT broker
    client = mqtt.Client(client_id=ident.node_id, clean_session=True)

    # Pass identifier to callbacks
    client.user_data_set(ident)

    client.username_pw_set(username, password)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    send_death_message(client, ident)
    client.connect(host, port)

    # Start the MQTT background thread
    client.loop_start()

    # Periodically send messages to the data topic
    while True:
        time.sleep(interval)
        if IS_CONNECTED:
            send_data(client, ident)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", default=1883, type=int, help="MQTT broker port")
    parser.add_argument("--username", default="user", help="MQTT username")
    parser.add_argument("--password", default="pass", help="MQTT password")
    parser.add_argument(
        "--interval", default=5, type=int, help="Send interval [seconds]"
    )
    parser.add_argument(
        "--group_id", default="Sparkplug_Devices", type=str, help="SP3 group id"
    )
    parser.add_argument("--node_id", default="[Hostname]", type=str, help="SP3 node id")
    args = parser.parse_args()

    identifier = Identifier(
        group_id=args.group_id,
        node_id=socket.gethostname() if args.node_id == "[Hostname]" else args.node_id,
    )
    run(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        interval=args.interval,
        ident=identifier,
    )
