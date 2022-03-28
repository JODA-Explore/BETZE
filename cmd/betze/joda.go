package main

import (
	"log"

	"github.com/JODA-Explore/BETZE/languages/joda"
)

func joda_connect(host string) *joda.JodaConnection {
	if len(host) > 0 {
		log.Println("JODA support enabled. Connecting...")
		connection, err := joda.Connect(host)
		if err != nil {
			log.Printf("Could not connect to JODA, disabling support: %v", err)
			return nil
		}
		return connection
	}
	return nil
}
