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
                byteIn = self.clientSocket.recv(100)
                if not byteIn :
                    print("<System>:",self.clientSocket.getpeername(),"doesn't send any data")
                    break
                command, topic, payload = unpack(byteIn)
                print("<server>: command {}:{} || topic: {} || paylod: {}".format(command,reverseCommandDict[command],topic,payload))
            except Exception as e:
                print("<System>:",self.clientSocket.getpeername(),e)
                break
        self.stop()

    def send(self, out):
        self.clientSocket.send(out)
    def stop(self):
        stopConnection(self.clientSocket.getpeername()[0])
        self.clientSocket.shutdown(socket.SHUT_RDWR)
        self.clientSocket.close()
        print("<System>: Thread stop")
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
    10:'Ack',
    11:'Nack'
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
        brokerList.append(addserv[0])
        threadGroup[addserv[0]] = ClientThread(clientSocket)
        threadGroup[addserv[0]].start()
        threadGroup[addserv[0]].send(strOut)
    except Exception as e:
        print("<System>: Something's wrong with "+str(addserv)+". Exception is "+str(e))

def stopConnection(ipServ):
    brokerList.remove(ipServ)
    threadGroup.pop(ipServ)

brokerList = list()
threadGroup = dict()
allCommand = ["sub","subscribe","pub","publish","unsub","unsubscribe",'exit','quit']
while True:
    try:
        temp = [v.strip() for  v in shlex.split(input())] 
        print(temp)
        temp[0] = temp[0].lower()
        if temp[0] not in allCommand: raise Exception("Command not found")
        if temp[0] in ['exit','quit']: raise KeyboardInterrupt
        if len(temp) < 2 : raise Exception("No ip or port")

        tempaddr = [ v.strip() for v in temp[1].split(':') ]
        if len(tempaddr) != 2 or not tempaddr[1].isnumeric():  raise Exception('Wrong ip or port')
        addserv = (tempaddr[0],int(tempaddr[1]))  # {servip,servport}

        if len(temp) < 3 : raise Exception("No topic")
        if len(temp) < 4 : payload = pack(temp[2],"")
        else : payload = pack(temp[2],temp[3])

        if temp[0] == 'pub' or temp[0] == 'publish': head = 'pub'
        elif temp[0] == 'sub' or temp[0] == 'subscribe' : head = 'sub'
        elif temp[0] == 'unsubscribe' or temp[0] == 'unsub' : head = 'unsub'
        else: raise Exception('Wrong syntax')

        print(headerPack(head,payload))
        print(unpack(headerPack(head,payload)))

        if addserv[0] not in brokerList:
            if head == 'unsub':
                raise Exception('You need to subscribe before unsubscribe')
            startConnection(addserv, headerPack(head,payload))
        else:
            threadGroup[addserv[0]].send(headerPack(head,payload))
        
    except (KeyboardInterrupt, SystemExit):
        print("<System>: Shutting down")
        sys.exit()
    except Exception as e:
        print("<System>:",e)

