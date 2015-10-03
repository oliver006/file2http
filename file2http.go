package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
)

var addr = flag.String("addr", "http://localhost", "HTTP address to make a request to.")
var method = flag.String("method", "GET", "HTTP request method.")
var contentType = flag.String("content-type", "application/octet-stream", "HTTP header content type.")
var numPublishers = flag.Int("n", runtime.NumCPU()*3, "Number of concurrent publishers")
var showVersion = flag.Bool("version", false, "print version string")

const VERSION = "0.4.0"

type Publisher struct {
	addr        string
	httpMethod  string
	contentType string
}

func (p *Publisher) Publish(msg string) error {

	var buf *bytes.Buffer
	endpoint := p.addr

	switch p.httpMethod {
	case "GET":
		endpoint = fmt.Sprintf(p.addr, url.QueryEscape(msg))
	default:
		buf = bytes.NewBuffer([]byte(msg))
	}
	client := &http.Client{}
	req, err := http.NewRequest(p.httpMethod, endpoint, buf)

	if p.contentType != "" {
		req.Header.Add("Content-Type", p.contentType)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	resp.Body.Close()
	return nil
}

func PublishLoop(waitGroup *sync.WaitGroup, pub Publisher, publishMsgs chan string) {
	for msg := range publishMsgs {
		err := pub.Publish(msg)
		if err != nil {
			log.Printf("ERROR: publishing '%s' - %s", msg, err.Error())
		}
	}
	waitGroup.Done()
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("file2http v%s\n", VERSION)
		return
	}

	httpMethod := strings.ToUpper(*method)
	if httpMethod == "GET" && strings.Count(*addr, "%s") != 1 {
		log.Fatal("Invalid get address - must be a format string")
	}

	msgsChan := make(chan string)
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(*numPublishers)
	for i := 0; i < *numPublishers; i++ {
		publisher := Publisher{addr: *addr, httpMethod: httpMethod}
		go PublishLoop(waitGroup, publisher, msgsChan)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Println(fmt.Sprintf("ERROR: %s", err))
			}
			break
		}
		line = strings.TrimSpace(line)
		msgsChan <- line
	}

	close(msgsChan)
	waitGroup.Wait()

	os.Exit(0)
}
