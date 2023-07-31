package test

import (
	"fmt"
	"testing"
	"time"
)

func TestIntervalConvert(t *testing.T) {
	skipCI(t)

	d := 2 * time.Second
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = time.Minute
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 16 * time.Minute
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 19 * time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 24 * time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))

	d = 24 * 5 * time.Hour
	fmt.Println(d, convertTimeDurationToElasticsearchInterval(d))
}

func convertTimeDurationToElasticsearchInterval(interval time.Duration) string {

	// Special case: get the time range and select the adaptive interval to provide up to the provided size points
	if interval == 0 {
		return ""
	}

	// duration from 0 to 60 minutes should be represented as minutes
	if interval < time.Minute {
		return fmt.Sprintf("%ds", interval/time.Second)
	}

	// duration from 0 to 60 minutes should be represented as minutes
	if interval < time.Hour {
		return fmt.Sprintf("%dm", interval/time.Minute)
	}

	// duration from 1 to 24 hours should be represented as hours
	if interval < 24*time.Hour {
		return fmt.Sprintf("%dh", interval/time.Hour)
	}

	return fmt.Sprintf("%dd", interval/(24*time.Hour))
}
