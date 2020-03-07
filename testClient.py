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
                strIn = self.clientSocket.recv(2048)
                if not strIn :
                    print("<System>:",self.clientSocket.getpeername(),"doesnt send anyd data")
                    break
                print("<server>:", strIn.decode('utf-8'))
            except Exception as e:
                print("<System>:",self.clientSocket.getpeername(),e)
                break
        self.stop()

    def send(self, strOut):
        self.clientSocket.send((strOut+'\n').encode('utf-8'))
    def stop(self):
        stopConnection(self.clientSocket.getpeername()[0])
        self.clientSocket.shutdown(socket.SHUT_RDWR)
        self.clientSocket.close()
        print("<System>: Thread stop")
        self._stop_event.set()


def startConnection(addserv, strOut):
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        clientSocket.settimeout(10)
        clientSocket.connect(addserv)
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
        temp[0] = temp[0].lower()
        if temp[0] not in allCommand: raise Exception("Command not found")
        if temp[0] in ['exit','quit']: raise KeyboardInterrupt
        if len(temp) < 2 : raise Exception("No ip or port")

        tempaddr = [ v.strip() for v in temp[1].split(':') ]
        if len(tempaddr) != 2 or not tempaddr[1].isnumeric():  raise Exception('Wrong ip or port')
        addserv = (tempaddr[0],int(tempaddr[1]))  # {servip,servport}
        strOut = temp[0]+" "+" ".join(temp[2:])

        if len(temp) < 3 : raise Exception("No topic")

        if len(temp) == 3:
            if temp[0] == "sub" or temp[0] == "subscribe":
                if addserv[0] not in brokerList:
                    startConnection(addserv, strOut)
                else:
                    threadGroup[addserv[0]].send(strOut)
            elif temp[0] == "unsub" or temp[0] == "unsubscribe":
                if addserv[0] not in brokerList:
                    raise Exception('You need to subscribe before unsubscribe')
                else:
                    threadGroup[addserv[0]].send(strOut)
            else: raise Exception('Wrong syntax')

        elif len(temp) == 4:
          
            if temp[0] == "pub" or temp[0] == "publish":
                if addserv[0] not in brokerList:
                    startConnection(addserv, strOut)
                else:
                    threadGroup[addserv[0]].send(strOut)
            else: raise Exception('Wrong syntax')

        else:
            raise Exception('Wrong input')
        print("<You>:", strOut)
    except (KeyboardInterrupt):
        print("<System>: Shutting down")
        sys.exit()
    except Exception as e:
        print("<System>:",e)

