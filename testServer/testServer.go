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
	go clientReciver()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		strOut := scanner.Text()
		for _, conn := range connlist {
			fmt.Fprintf(conn, strOut)
		}
	}
}

func clientReciver() {
	ServPort := ":50000"
	ln, _ := net.Listen("tcp", "127.0.0.1"+ServPort)
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
		temp := strings.Split(strIn, " ")
		if strings.ToLower(temp[0]) == "sub" || strings.ToLower(temp[0]) == "subscribe" {
			fmt.Print("<System>: ", conn.RemoteAddr().String(), " has subscribe to ", temp[1])
			db.updateClientMap(conn.RemoteAddr().String(), strings.TrimSpace(temp[1]))
			db.publishRetainValue(conn.RemoteAddr().String(), strings.TrimSpace(temp[1]))
		} else if strings.ToLower(temp[0]) == "pub" || strings.ToLower(temp[0]) == "publish" {
			fmt.Print("<System>: ", conn.RemoteAddr().String(), " has publish topic ", temp[1], " ", temp[2])
			db.updateRetainMap(strings.TrimSpace(temp[1]), strings.TrimSpace(temp[2]))
			db.publish(strings.TrimSpace(temp[1]), strings.TrimSpace(temp[2]))
		}
	}
}

func (db *safeDB) updateRetainMap(topic string, value string) {
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

func (db *safeDB) updateClientMap(clientIP string, topic string) {
	db.m.Lock()
	defer db.m.Unlock()
	db.clientMap[topic] = append(db.clientMap[topic], clientIP)

}

func (db *safeDB) publishRetainValue(clientIP string, topic string) {
	db.m.Lock()
	defer db.m.Unlock()
	if val, has := db.retainMap[topic]; has {
		fmt.Fprintf(connlist[clientIP], topic+" "+val)
	}

}
