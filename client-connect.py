import paho.mqtt.client as mqtt
import time
import dns.resolver as dns

#broker="comp3310.ddns.net"
ip=""

for i in dns.query("comp3310.ddns.net"):
    ip = str(i).split('\n', 1)[0]

def on_log(client, userdata, level, buf):
    print("log" + buf)

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("connected OK")
    else:
        print("bad connection returned code=", rc)

def on_message(client, userdata, msg):
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8"))
    print("message received", m_decode)

broker = ip
client = mqtt.Client("3310-u6051965")

client.on_connect=on_connect
client.on_log=on_log
client.username_pw_set("students", "33106331")
print("connecting to broker ", broker)
client.connect(broker)
client.loop_start()
client.subscribe("counter/slow/q0")
client.on_message = on_message

time.sleep(4)

client.loop_stop()

client.disconnect()

print("done" + str(ip))