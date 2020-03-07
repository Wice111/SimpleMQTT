import socket
import sys
import threading


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
allCommand = ["sub","subsribe","pub","publish","unsub","unsubsribe"]
while True:
    try:
        temp = [v.strip() for  v in input().replace("'",' ').strip().split()] 
        if len(temp) == 0:
            pass

        elif len(temp) == 4:
            temp[0] = temp[0].lower()
            if temp[0] not in allCommand: raise Exception("Command not found")
            
            tempaddr = [ v.strip() for v in temp[1].split(':') ]
            if len(tempaddr) != 2 and not tempaddr[1].isnumeric():  raise Exception('Wrong ip or port')
         
            addserv = (tempaddr[0],int(tempaddr[1]))  # {servip,servport}
        
            strOut = temp[0]+" "+" ".join(temp[2:])
            if temp[0] == "sub" or temp[0] == "subsribe":
                if addserv[0] not in brokerList:
                    startConnection(addserv, strOut)
                else:
                    threadGroup[addserv[0]].send(strOut)
            elif temp[0] == "pub" or temp[0] == "publish":
                if addserv[0] not in brokerList:
                    startConnection(addserv, strOut)
                else:
                    threadGroup[addserv[0]].send(strOut)
            elif temp[0] == "unsub" or temp[0] == "unsubsribe":
                if addserv[0] not in brokerList:
                    raise Exception('You need to subscribe before unsubsribe')
                else:
                    threadGroup[addserv[0]].send(strOut)
            else:
                raise Exception('Command not found')

            print("<You>:", strOut)

        else:
            raise Exception('Wrong syntax')

    except (KeyboardInterrupt, SystemExit):
        print("<System>: Shutting down")
        sys.exit()
    except Exception as e:
        print("<System>:",e)
