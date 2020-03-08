package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"sync"
)

type safeDB struct {
	retainMap map[string]string
	clientMap map[string][]string
	m         sync.Mutex
}

var db *safeDB
var connlist map[string]net.Conn

func main() {
	connlist = make(map[string]net.Conn)
	db = &safeDB{}
	db.retainMap = make(map[string]string)
	db.clientMap = make(map[string][]string)
	scanner := bufio.NewScanner(os.Stdin)
	var strIn string
	for {
		fmt.Print("<System>: Please input server IP address and port > ")
		scanner.Scan()
		strIn = scanner.Text()

		validAddr, _ := regexp.MatchString(`^[0-9]+(?:\.[0-9]+){3}:[0-9]+$`, strIn)
		if validAddr {
			break
		}
	}
	go clientReceiver(strIn)
	for {
		scanner.Scan()
		strOut := scanner.Text()
		for _, conn := range connlist {
			fmt.Fprintf(conn, strOut)
		}
	}
}

func clientReceiver(servAddr string) {
	ln, _ := net.Listen("tcp", servAddr)
	fmt.Println("<System>: Now listening at ", servAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln(err)
			os.Exit(1)
		}
		fmt.Println("<System>: ", conn.RemoteAddr().String(), "has connected")
		connlist[conn.RemoteAddr().String()] = conn
		go clientHandler(conn)
	}
}

func clientHandler(conn net.Conn) {
	for {
		strIn := make([]byte, 70000)
		strInLen, _ := conn.Read(strIn)

		//check for disconnect
		if strInLen == 0 {
			break
		}

		//extract topic
		topicLen := int(strIn[1])
		topic := string(strIn[2:(2 + topicLen)])

		//extract payload
		payloadLen := (int(strIn[2+topicLen]) * 256) + int(strIn[3+topicLen])
		payload := string(strIn[(4 + topicLen):(4 + topicLen + payloadLen)])

		//processing
		//publish = 2, puback = 3
		//subscribe = 4, suback = 5
		//unsubscribe = 6, unsuback = 7
		//ack = 10, nack = 11
		if strIn[0] == 2 {
			if payloadLen != 0 {
				db.updateRetain(topic, payload)
				db.publish(topic, payload)
				fmt.Println("<PUB>: ", conn.RemoteAddr().String(), " just published '", payload, "' to topic ", topic)
				sendData(conn, 3, strIn[2:(2+topicLen)], nil)
			} else {
				fmt.Println("<PUB>: ", conn.RemoteAddr().String(), " published without a payload to topic ", topic)
				sendData(conn, 11, strIn[2:(2+topicLen)], []byte("Payload cannot be null."))
			}
		} else if strIn[0] == 4 {
			if db.addClient(conn.RemoteAddr().String(), topic) {
				fmt.Println("<SUB>: ", conn.RemoteAddr().String(), " just subscribed to topic ", topic)
				sendData(conn, 5, strIn[2:(2+topicLen)], nil)
				db.publishRetainValue(conn.RemoteAddr().String(), topic)
			} else {
				fmt.Println("<SUB>: ", conn.RemoteAddr().String(), " tried to resubscribe to topic ", topic)
				sendData(conn, 11, strIn[2:(2+topicLen)], []byte("You're already subscribed to this topic."))
			}
		} else if strIn[0] == 6 {
			if db.deleteClient(conn.RemoteAddr().String(), topic) {
				fmt.Println("<UNSUB>: ", conn.RemoteAddr().String(), " just unsubscribed from topic ", topic)
				sendData(conn, 7, strIn[2:(2+topicLen)], nil)
			} else {
				fmt.Println("<UNSUB>: ", conn.RemoteAddr().String(), " tried to unsubscribed from topic ", topic, " without subscription")
				sendData(conn, 11, strIn[2:(2+topicLen)], []byte("You have to subscribe to this topic first."))
			}
		} else {
			sendData(conn, 11, strIn[2:(2+topicLen)], []byte("Unknown command."))
		}
	}
	conn.Close()
	db.deleteAllClient(conn.RemoteAddr().String())
	fmt.Print("<System>: ", conn.RemoteAddr().String(), " has disconnected\n")
}

func (db *safeDB) updateRetain(topic string, value string) {
	db.m.Lock()
	defer db.m.Unlock()
	db.retainMap[topic] = value
}

func (db *safeDB) publish(topic string, value string) {
	db.m.Lock()
	defer db.m.Unlock()
	for _, clientIP := range db.clientMap[topic] {
		if _, has := connlist[clientIP]; has {
			sendData(connlist[clientIP], 2, []byte(topic), []byte(value))
		}
	}
}

func (db *safeDB) addClient(clientIP string, topic string) bool {
	db.m.Lock()
	defer db.m.Unlock()
	if _, found := find(db.clientMap[topic], clientIP); found {
		return false
	}
	db.clientMap[topic] = append(db.clientMap[topic], clientIP)
	return true

}

func (db *safeDB) deleteClient(clientIP string, topic string) bool {
	db.m.Lock()
	defer db.m.Unlock()
	if _, has := db.clientMap[topic]; has {
		if deleteSliceElm(db.clientMap[topic], clientIP) {
			return true
		}
	}
	return false
}

func (db *safeDB) publishRetainValue(clientIP string, topic string) {
	db.m.Lock()
	defer db.m.Unlock()
	if val, has := db.retainMap[topic]; has {
		sendData(connlist[clientIP], 2, []byte(topic), []byte(val))
		fmt.Println("<PUB>: Sending retain value of topic ", topic, " to new subscriber.")
	}

}

func deleteSliceElm(slice []string, item string) bool {
	if i, found := find(slice, item); found {
		slice[i] = slice[len(slice)-1]
		slice[len(slice)-1] = ""
		slice = slice[:len(slice)-1]
		return true
	}
	return false
}

func find(slice []string, item string) (int, bool) {
	for i, it := range slice {
		if it == item {
			return i, true
		}
	}
	return -1, false
}

func (db *safeDB) deleteAllClient(cilentIP string) {
	delete(connlist, cilentIP)
	for key := range db.clientMap {
		db.deleteClient(cilentIP, key)
	}

}

func sendData(conn net.Conn, cmd byte, header []byte, payload []byte) {
	var strOut []byte

	strOut = append(strOut, cmd)

	headerSize := make([]byte, 8)
	binary.BigEndian.PutUint64(headerSize, uint64(len(header)))
	strOut = append(strOut, headerSize[7])

	strOut = append(strOut, header...)

	payloadSize := make([]byte, 8)
	binary.BigEndian.PutUint64(payloadSize, uint64(len(payload)))
	strOut = append(strOut, payloadSize[6:]...)

	if payload == nil {
		conn.Write(strOut[0:(4 + len(header))])
	} else {
		strOut = append(strOut, payload...)
		conn.Write(strOut[0:(4 + len(header) + len(payload))])
	}
}
