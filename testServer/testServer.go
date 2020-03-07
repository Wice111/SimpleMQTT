package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
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
	fmt.Println("<System>: Please input server port")
	scanner.Scan()
	strin := scanner.Text()
	go clientReciver(strin)
	for {
		scanner.Scan()
		strOut := scanner.Text()
		for _, conn := range connlist {
			fmt.Fprintf(conn, strOut)
		}
	}
}

func clientReciver(servport string) {
	servport = ":" + servport
	ln, _ := net.Listen("tcp", "127.0.0.1"+servport)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln(err)
			os.Exit(1)
		}
		fmt.Println("<System>:", conn.RemoteAddr().String(), "has connected")
		connlist[conn.RemoteAddr().String()] = conn
		go clientHandler(conn)
	}
}

func clientHandler(conn net.Conn) {
	for {
		strIn, _ := bufio.NewReader(conn).ReadString('\n')
		if strIn == "" {
			break
		}
		temp := strings.Split(strIn, " ")
		if strings.ToLower(temp[0]) == "sub" || strings.ToLower(temp[0]) == "subscribe" {
			if db.addClient(conn.RemoteAddr().String(), strings.TrimSpace(temp[1])) {
				db.publishRetainValue(conn.RemoteAddr().String(), strings.TrimSpace(temp[1]))
				fmt.Print("<System>: ", conn.RemoteAddr().String(), " has subscribed to ", temp[1])
				fmt.Fprintf(conn, "Subscribe confirmed")
			} else {
				fmt.Print("<System>: ", conn.RemoteAddr().String(), " has already been subscribed to ", temp[1])
				fmt.Fprintf(conn, "You has already been subscribeb to "+strings.TrimSpace(temp[1]))
			}

		} else if strings.ToLower(temp[0]) == "pub" || strings.ToLower(temp[0]) == "publish" {
			fmt.Print("<System>: ", conn.RemoteAddr().String(), " has published topic ", temp[1], " ", temp[2])
			db.updateRetain(strings.TrimSpace(temp[1]), strings.TrimSpace(temp[2]))
			db.publish(strings.TrimSpace(temp[1]), strings.TrimSpace(temp[2]))
			fmt.Fprintf(conn, "Published confirmed")
		} else if strings.ToLower(temp[0]) == "unsub" || strings.ToLower(temp[0]) == "unsubsribe" {
			if db.deleteClient(conn.RemoteAddr().String(), strings.TrimSpace(temp[1])) {
				fmt.Print("<System>: ", conn.RemoteAddr().String(), " has unsubsribed topic ", temp[1])
				fmt.Fprintf(conn, "Unsubsribe confirmed")
			} else {
				fmt.Print("<System>: ", conn.RemoteAddr().String(), " cant unsubsribe topic ", temp[1])
				fmt.Fprintf(conn, "Cant unsubsribe")
			}
		}
	}
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
		fmt.Fprintf(connlist[clientIP], topic+" "+value)
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
		fmt.Fprintf(connlist[clientIP], topic+" "+val)
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
	for key, _ := range db.clientMap {
		db.deleteClient(cilentIP, key)
	}

}
