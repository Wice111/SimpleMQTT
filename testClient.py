import socket
import sys
import threading


class ClientThread(threading.Thread):
    def __init__(self, clientSocket):
        self.clientSocket = clientSocket
        threading.Thread.__init__(self)

    def run(self):
        while True:
            strIn = self.clientSocket.recv(2048)
            print("<server>:", strIn.decode('utf-8'))

    def send(self, strOut):
        self.clientSocket.send((strOut+'\n').encode('utf-8'))


def startConnection(ipServ, strOut):
    SERV_PORT = 50000
    addr = (ipServ, SERV_PORT)
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        clientSocket.settimeout(10)
        clientSocket.connect(addr)
        clientSocket.settimeout(None)
        print("<System>: Server connected: " + str(addr[0])+':' + str(addr[1]))
        brokerList.append(ipServ)
        threadGroup[ipServ] = ClientThread(clientSocket)
        threadGroup[ipServ].start()
        threadGroup[ipServ].send(strOut)
    except Exception as e:
        print("<System>: Something's wrong with "+str(addr)+". Exception is "+str(e))


brokerList = list()
threadGroup = dict()
while True:
    temp = input().split(' ')
    try:
        ipServ = temp[1]
        strOut = temp[0]+' '+" ".join(temp[2:])
        if temp[0].lower() == "sub" or temp[0].lower() == "subscribe":
            if ipServ not in brokerList:
                startConnection(ipServ, strOut)
            else:
                threadGroup[ipServ].send(strOut)
        elif temp[0].lower() == "pub" or temp[0].lower() == "publish":
            if ipServ not in brokerList:
                print("There is no Broker at", str(ipServ))
            else:
                threadGroup[ipServ].send(strOut)
        else:
            raise Exception('Wrong input')
        print("<You>:", strOut)
    except Exception as e:
        print("<System>:",e)
