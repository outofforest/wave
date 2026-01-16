package main

import (
	"github.com/outofforest/proton"
	"github.com/outofforest/wave/test/wire2"
)

//go:generate go run .
func main() {
	proton.Generate("../types.proton.go",
		proton.Message[wire2.Msg1](),
		proton.Message[wire2.Msg2](),
	)
}
