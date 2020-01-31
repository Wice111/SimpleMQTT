package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

var connlist []net.Conn

func main() {
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
	ln, _ := net.Listen("tcp", "localhost"+ServPort)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln(err)
			os.Exit(1)
		}
		connlist = append(connlist, conn)
		go clientHandler(conn)
	}
}

func clientHandler(conn net.Conn) {
	fmt.Println("Connected:", conn.RemoteAddr().String())
	for {
		strIn, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print("<", conn.RemoteAddr(), ">: ", strIn)
	}
}
