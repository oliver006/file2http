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

const VERSION = "0.4.0"

type Publisher struct {
	addr             string
	httpMethod       string
	contentType      string
	fieldTransformer *TimestampTransformer
	urlTransformer   *JsonURLTransformer
}

func (p *Publisher) Publish(msg string) error {

	var buf *bytes.Buffer
	endpoint := p.addr

	var err error
	if p.fieldTransformer != nil {
		msg, err = p.fieldTransformer.Transform(msg)
		if err != nil {
			log.Printf("stupid oliver: %", err)
			return err
		}
	}

	if p.urlTransformer != nil {
		endpoint, err = p.urlTransformer.Transform(msg, endpoint)
		if err != nil {
			log.Printf("stupid: %", err)
			return err
		}
	}

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
	addr := flag.String("addr", "http://localhost", "HTTP address to make a request to.")
	method := flag.String("method", "GET", "HTTP request method.")
	contentType := flag.String("content-type", "application/octet-stream", "HTTP header content type.")
	numPublishers := flag.Int("n", runtime.NumCPU()*3, "Number of concurrent publishers")
	transJsonURL := flag.String("transform-json-url", "", "")
	transTSFields := flag.String("transform-ts-fields", "", "")
	showVersion := flag.Bool("version", false, "print version string")
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

	publisher := Publisher{addr: *addr, httpMethod: httpMethod, contentType: *contentType}
	if *transJsonURL != "" {

		publisher.urlTransformer = CreateJsonURLTransformer(*transJsonURL)
	}

	if *transTSFields != "" {
		publisher.fieldTransformer = &TimestampTransformer{fields: strings.Split(*transTSFields, ",")}
	}

	for i := 0; i < *numPublishers; i++ {

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
