import socket
import sys
import threading
import shlex

class ClientThread(threading.Thread):
    def __init__(self, clientSocket):
        self.clientSocket = clientSocket
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()
        self.setDaemon(True)
    def run(self):
        while True:
            try:
                byteIn = self.clientSocket.recv(66000)
                command, topic, payload = unpack(byteIn)

                if command == CommandDict['nack'] :
                    print("<Broker>: [Error message] ",payload)
                elif command == CommandDict['puback']:
                    print("<Broker>: Publish topic['{}'] complete".format(topic))
                elif command == CommandDict['suback']:
                    print("<Broker>: Subscribe topic['{}'] complete".format(topic))
                elif command == CommandDict['unsuback']:
                    print("<Broker>: Subscribe topic['{}']".format(topic))
                elif command == CommandDict['pub']:
                    print("<Broker>: topic['{}']: {}".format(topic,payload))
                elif command == CommandDict['sub']:
                    pass
                elif command == CommandDict['unsub']:
                    pass
                elif command == CommandDict['disconnect']:
                    pass
                else:
                    print("unKnown: {} {} {} ",command, topic, payload)

                # if not byteIn :
                #     print("<System>:",self.clientSocket.getpeername(),"doesn't send any data")
                #     break     
            except Exception as e:
                print("<System Error>:",self.clientSocket.getpeername(),e)
                break
        self.stop()

    def send(self, out):
        self.clientSocket.send(out)
    def stop(self):
        print("<System>: Thread stop at ip: {} port: {}".format(self.clientSocket.getpeername()[0],self.clientSocket.getpeername()[1])
        )
        stopConnection(self.clientSocket.getpeername())
        self.clientSocket.shutdown(socket.SHUT_RDWR)
        self.clientSocket.close()
        self._stop_event.set()

CommandDict = {
    'pub'       : 2,
    'puback'    : 3,
    'sub'       : 4,
    'suback'    : 5,
    'unsub'     : 6,
    'unsuback'  : 7,
    'disconnect': 8,
    'ack'       : 10,
    'nack'      : 11
}

reverseCommandDict = {
    2:'publish',
    3:'publish ack', 
    4:'subscribe',      
    5:'subscribe ack',   
    6:'unsubscribe',    
    7:'unsubscribe ack', 
    8:'disconnect',
    10:'ack',
    11:'nack'
}

def write2byte(data):
    return bytes([data // 256, data % 256])

def Command(command_):
    return bytes([CommandDict[command_]])

def headerPack(cmd,payload):
    buffer = Command(cmd)
    buffer += payload
    return buffer

def pack(topicName="", payload=""):
    buffer = bytes([len(topicName)])
    buffer += bytes(topicName,'utf-8')

    # message length
    buffer += write2byte(len(payload))
    buffer += bytes(payload,'utf-8') 
    return buffer

def unpack(data):
    command = int(data[0])
    Ntopic =  int(data[1])
    Npayload = int(data[2+Ntopic])*256 + int(data[3+Ntopic])
    topicName = data[2:2+Ntopic].decode('utf8')
    payload = data[4+Ntopic:4+Ntopic+Npayload].decode('utf8')
    return command,topicName,payload
        
def startConnection(addserv, strOut):
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # can connect
        clientSocket.settimeout(10)
        clientSocket.connect(addserv)

        # time out for messages
        clientSocket.settimeout(None)
        print("<System>: Server connected: " + str(addserv[0])+':' + str(addserv[1]))
        brokerList.append(addserv)
        threadGroup[addserv] = ClientThread(clientSocket)
        threadGroup[addserv].start()
        threadGroup[addserv].send(strOut)
    except Exception as e:
        print("<System>: Something's wrong with "+str(addserv)+". Exception is "+str(e))

def stopConnection(addserv):
    brokerList.remove(addserv)
    threadGroup.pop(addserv)

brokerList = list()
threadGroup = dict()
HELP = ''' 
Command :
Usage: 
    <command> [ip : port] [topic] [value]

The commands are
  <short>   <full command>
    pub     publish
    sub     subscribe
    unsub   unsubscribe
    exit
    quit
    help                       '''
print(HELP)
print('Try to connect to server at 127.0.0.1:50000')
while True:
    try:
        temp = [v.strip() for  v in shlex.split(input())] 
        if len(temp) < 1 : continue
        temp[0] = temp[0].lower()
        if temp[0] in ['exit','quit']: raise KeyboardInterrupt 
        elif temp[0] == 'help': 
            print(HELP)
            continue
        elif temp[0] == 'pub' or temp[0] == 'publish': head = 'pub'
        elif temp[0] == 'sub' or temp[0] == 'subscribe' : head = 'sub'
        elif temp[0] == 'unsubscribe' or temp[0] == 'unsub' : head = 'unsub'
        else: raise Exception("Command not found")

        if len(temp) < 2 : raise Exception("No ip or port")

        tempaddr = [ v.strip() for v in temp[1].split(':') ]
        if len(tempaddr) != 2 or not tempaddr[1].isnumeric():  raise Exception('Wrong ip or port')
        addserv = (tempaddr[0],int(tempaddr[1]))  # {servip,servport}

        if len(temp) < 3 : raise Exception("No topic")
        if len(temp) < 4 : payload = pack(temp[2],"")
        else : payload = pack(temp[2],temp[3])

        if addserv not in brokerList:
            startConnection(addserv, headerPack(head,payload))
        else:
            threadGroup[addserv].send(headerPack(head,payload))
        
    except (KeyboardInterrupt, SystemExit):
        print("<System>: Shutting down")
        sys.exit()
    except Exception as e:
        print("<System ee >:",e)

