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

var connlist []net.Conn

type safeDB struct {
	retainDB map[string]string
	clientDB map[string][]string
	m        sync.Mutex
}

func main() {
	db := &safeDB{}
	db.clientDB = make(map[string]string)
	db.retainDB = make(map[string][]string)
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
		fmt.Println("Connected:", conn.RemoteAddr().String())
		connlist = append(connlist, conn)
		go clientHandler(conn)
	}
}

func clientHandler(conn net.Conn) {
	for {
		strIn, _ := bufio.NewReader(conn).ReadString('\n')
		temp := strings.Split(strIn, " ")
		if strings.ToLower(temp[0]) == "sub" || strings.ToLower(temp[0]) == "subscribe" {
			
		}
		fmt.Print("<", conn.RemoteAddr(), ">: ", strIn)
	}
}

func (db *safeDB) updateRetainDB(ipClient string,topic string){
	db.m.Lock()
	defer db.m.Unlock()
	db.clientDB[]
}