package main

type OutboxStream interface {
	FetchEvents() (chan string, error)
}
