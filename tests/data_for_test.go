// Data model and testing data
package test

import (
	"encoding/json"
	"fmt"
	. "github.com/go-yaaf/yaaf-common/entity"
	"time"
)

// region Heroes Test Model --------------------------------------------------------------------------------------------
type Hero struct {
	BaseEntity
	Key      string  `json:"key"`      // Key
	Num      int     `json:"num"`      // Number
	Name     string  `json:"name"`     // Name
	Type     string  `json:"type"`     // Type
	Color    string  `json:"color"`    // Color
	Strength float64 `json:"strength"` // Strength (1..100)
	Brain    float64 `json:"brain"`    // Strength (1..100)
}

func (a *Hero) TABLE() string { return "hero-{{accountId}}-{{year}}.{{month}}" }
func (a *Hero) NAME() string  { return a.Name }
func (a *Hero) KEY() string   { return a.Key }

func (a *Hero) String() string {
	if bytes, err := json.Marshal(a); err != nil {
		return err.Error()
	} else {
		return string(bytes)
	}
}

func NewHero() Entity {
	return &Hero{}
}

func NewHero1(id string, num int, name, typ, color string) Entity {
	return &Hero{
		BaseEntity: BaseEntity{Id: id, CreatedOn: Now(), UpdatedOn: Now()},
		Num:        num,
		Name:       name,
		Type:       typ,
		Color:      color,
	}
}

// Get random documents over the last 30 days
func GetRandomListOfHeroes(size int) []Entity {
	end := Now()
	start := end.Add(-24 * 31 * time.Hour)
	delta := (int64(end) - int64(start)) / int64(size)

	result := make([]Entity, 0)

	for i := 1; i <= size; i++ {

		ts := int64(start) + (int64(i-1) * delta) + rnd.Int63n(delta)
		hero := NewHero()
		hero.(*Hero).Id = fmt.Sprintf("%d", i)
		hero.(*Hero).CreatedOn = Timestamp(ts)
		hero.(*Hero).UpdatedOn = Timestamp(ts)

		hero.(*Hero).Key = keys[rnd.Intn(len(keys))]
		//hero.(*Hero).Key = "disney"
		hero.(*Hero).Num = rnd.Intn(8)
		hero.(*Hero).Name = getRandomName()
		hero.(*Hero).Color = colors[rnd.Intn(len(colors))]
		hero.(*Hero).Type = types[rnd.Intn(len(types))]
		hero.(*Hero).Strength = float64(rnd.Intn(100))
		hero.(*Hero).Brain = float64(rnd.Intn(100))

		result = append(result, hero)
	}
	return result
}

func getRandomName() string {
	n1 := signs[rnd.Intn(len(signs))]
	n2 := signs[rnd.Intn(len(signs))]
	name := fmt.Sprintf("%s %s", n1, n2)
	return name
}

var list_of_heroes = []Entity{
	NewHero1("1", 1, "Ant Man", "alpha", "white"),
	NewHero1("2", 2, "Aqua Man", "beta", "black"),
	NewHero1("3", 3, "Quine Esther", "gamma", "red"),
	NewHero1("4", 4, "Bat Girl", "delta", "green"),
	NewHero1("5", 5, "Bat Man", "epsilon", "blue"),
	NewHero1("6", 6, "Bat Woman", "zeta", "yellow"),
	NewHero1("7", 7, "Black Canary", "eta", "brown"),
	NewHero1("8", 8, "Black Panther", "theta", "purple"),
	NewHero1("9", 9, "Captain America", "iota", "grey"),
	NewHero1("10", 10, "Captain Marvel", "kappa", "orange"),
	NewHero1("11", 11, "Cat Woman", "lambda", "magenta"),
	NewHero1("12", 12, "Conan the Barbarian", "mu", "maroon"),
	NewHero1("13", 13, "Daredevil Kong", "xi", "white"),
	NewHero1("14", 14, "Doctor Strange", "omicron", "black"),
	NewHero1("15", 15, "Elektra King", "pi", "red"),
	NewHero1("16", 16, "Ghost Rider", "rho", "green"),
	NewHero1("17", 17, "Green Arrow", "sigma", "blue"),
	NewHero1("18", 18, "Green Lantern", "tau", "yellow"),
	NewHero1("19", 19, "Hawkeye Meg", "upsilon", "brown"),
	NewHero1("20", 20, "Hell Boy", "phi", "purple"),
	NewHero1("21", 21, "Iron Man", "chi", "grey"),
	NewHero1("22", 22, "Robin Hood", "psi", "orange"),
	NewHero1("23", 23, "Spider Man", "omega", "magenta"),
	NewHero1("24", 24, "Super Girl", "lambda", "maroon"),
	NewHero1("25", 25, "Superman Boy", "eta", "white"),
	NewHero1("26", 26, "King Thor", "delta", "black"),
	NewHero1("27", 27, "The Wasp", "rho", "red"),
	NewHero1("28", 28, "Wolverine Man", "iota", "green"),
	NewHero1("29", 29, "Wonder Woman", "sigma", "blue"),
	NewHero1("30", 30, "X Man", "alpha", "yellow"),
}

// endregion
