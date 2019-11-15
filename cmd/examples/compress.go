package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pierrec/lz4/v4"
)

func main() {
	src, err := os.Open("/home/chausat/src")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer src.Close()

	dest, err := os.Create("/home/chausat/dest")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer dest.Close()

	w := lz4.NewWriter(dest)

	br := bufio.NewReader(src)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		text := strings.Split(string(line), "service.mq.LogMessageListener -")
		if len(text) == 2 {
			w.Write([]byte(text[1]))
		}
	}

	w.Close()
}
