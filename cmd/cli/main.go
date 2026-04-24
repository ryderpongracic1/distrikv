package main

import (
	"os"

	"github.com/ryderpongracic1/distrikv/cli"
)

// version is set at build time via:
//
//	go build -ldflags="-X main.version=$(git describe --tags --always)"
var version = "dev"

func main() {
	c := cli.New(version)
	if err := c.Execute(); err != nil {
		cli.HandleErr(err)
		os.Exit(1) // unreachable — HandleErr calls os.Exit; satisfies compiler
	}
}
