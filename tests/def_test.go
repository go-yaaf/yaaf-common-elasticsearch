package test

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-yaaf/yaaf-common/entity"
)

func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
}

var rnd = rand.New(rand.NewSource(time.Now().UnixMilli()))
var keys = []string{"m", "w", "a"}
var types = []string{"circle", "triangle", "square", "pentagon"}
var greek = []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "mu", "xi", "omicron", "pi", "rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega"}
var signs = []string{"Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliett", "Kilo", "Lima", "Mike", "November", "Oscar", "Papa", "Quebec", "Romeo", "Sierra", "Tango", "Uniform", "Victor", "Whiskey", "X-ray", "Yankee", "Zulu"}
var colors = []string{"white", "black", "red", "green", "blue", "yellow", "brown", "purple", "grey", "orange", "magenta", "maroon"}

func relativeToAbsoluteTime(input entity.Timestamp) entity.Timestamp {
	if input < 0 {
		return entity.Now() + input
	} else {
		return input
	}
}
