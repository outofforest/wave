package main

import (
	"github.com/outofforest/proton"
	"github.com/outofforest/wave/test/wire1"
)

//go:generate go run .
func main() {
	proton.Generate("../types.proton.go",
		proton.Message(wire1.Msg1{}),
		proton.Message(wire1.Msg2{}),
	)
}
