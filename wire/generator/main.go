package main

import (
	"github.com/outofforest/proton"
	"github.com/outofforest/wave/wire"
)

//go:generate go run .
func main() {
	proton.Generate("../types.proton.go",
		proton.Message(wire.Hello{}),
		proton.Message(wire.Header{}),
	)
}
