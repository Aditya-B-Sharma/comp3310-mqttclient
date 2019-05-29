import paho.mqtt.client as mqtt
import time
import dns.resolver as dns
import statistics
#broker="comp3310.ddns.net"
ip=""
slowmessagesq0 = []
slowmessagesq1 = []
slowmessagesq2 = []
fastmessagesq0 = []
fastmessagesq1 = []
fastmessagesq2 = []
dupes = {
        "counter/slow/q0" : 0,
        "counter/slow/q1" : 0,
        "counter/slow/q2" : 0,
        "counter/fast/q0" : 0,
        "counter/fast/q1" : 0,
        "counter/fast/q2" : 0,
}

times = {
        "counter/slow/q0" : [],
        "counter/slow/q1" : [],
        "counter/slow/q2" : [],
        "counter/fast/q0" : [],
        "counter/fast/q1" : [],
        "counter/fast/q2" : [],
}

ooo = []

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
    #print(time.time())
    print(topic)
    dupes[topic] = dupes[topic] + int(msg.dup)
    times[topic].append((round(time.time()*1000, 2)))
    print("msg.dup: ", msg.dup)
    global slowmessagesq0, slowmessagesq1, slowmessagesq2, fastmessagesq0, fastmessagesq1, fastmessagesq2
    #print("topic" + topic)
    m_decode=str(msg.payload.decode("utf-8"))
    if topic == "counter/slow/q0": 
        slowmessagesq0.append(m_decode);
    elif topic == "counter/slow/q1":
        slowmessagesq1.append(m_decode);
    elif topic == "counter/slow/q2": 
        slowmessagesq2.append(m_decode);
    elif topic == "counter/fast/q0":
        fastmessagesq0.append(m_decode);
    elif topic == "counter/fast/q1": 
        fastmessagesq1.append(m_decode);
    elif topic == "counter/fast/q2":
        fastmessagesq2.append(m_decode);
    #print("message received", m_decode)

def messagerate(topic, messagelist, time):
        print(topic, "has a message rate of", round(len(messagelist)/time, 2), "messages per second.")
        amountmsg = len(messagelist)
        current = int(messagelist[0])
        loss = 0
        i = 1
        lost = []
        differences = []
        while i < amountmsg:
                if messagelist[i]==0: 
                        current=messagelist[i]
                        i += 1
                        continue
                if int(messagelist[i]) < current:
                        ooo.append(int(messagelist[i]))
                elif int(messagelist[i])-1 == current:
                        current=int(messagelist[i])
                else:
                        for x in range(current, int(messagelist[i])):
                                lost.append(x)
                                loss += 1
                        current=int(messagelist[i])
                i += 1
        if topic.__contains__("fast"):
                j = 0
                while j < len(times[topic])-1:
                        differences.append(times[topic][j+1] - times[topic][j])
                        j += 1

        print("The loss rate for this topic is", round(loss/(amountmsg+loss), 2), "%.", "There are a total of", len(lost), "lost/misplaced messages.")
        print("The amount of duplicates for topic", topic, "are", dupes[topic], ".")
        print("The Out-Of-Order rate for this topic is", round(len(ooo)/amountmsg), ".")
        print("The mean inter-message gap for this topic is", statistics.mean(differences), "milliseconds.")
        print("The gap variation is", statistics.stdev(differences), "milliseconds.")
        
        print('\n')
                

broker = ip
client = mqtt.Client("3310-u6051965")

client.on_connect=on_connect
# client.on_log=on_log
client.username_pw_set("students", "33106331")
print("connecting to broker ", broker)
client.connect(broker)
client.loop_start()
client.on_message = on_message
# client.subscribe("counter/slow/q0", qos=0)
# time.sleep(15)
# client.unsubscribe("counter/slow/q0")
# client.subscribe("counter/slow/q1", qos=1)
# time.sleep(15)
# client.unsubscribe("counter/slow/q1")
# client.subscribe("counter/slow/q2", qos=2)
# time.sleep(15)
# client.unsubscribe("counter/slow/q2")
# client.subscribe("counter/fast/q0", qos=0)
# time.sleep(15)
# client.unsubscribe("counter/fast/q0")
# client.subscribe("counter/fast/q1", qos=1)
# time.sleep(15)
# client.unsubscribe("counter/fast/q1")
client.subscribe("counter/fast/q2", qos=2)
time.sleep(5)
client.unsubscribe("counter/fast/q2")

client.loop_stop()
client.disconnect()

print("done" + str(ip))
# messagerate("counter/slow/q0", slowmessagesq0, 15)
# messagerate("counter/slow/q1", slowmessagesq1, 15)
# messagerate("counter/slow/q2", slowmessagesq2, 15)
# messagerate("counter/fast/q0", fastmessagesq0, 15)
# messagerate("counter/fast/q1", fastmessagesq1, 15)
messagerate("counter/fast/q2", fastmessagesq2, 5)
print(times)