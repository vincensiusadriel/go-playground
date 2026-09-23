package main

import (
	"fmt"
	"time"

	"github.com/vincensiusadriel/go-playground/pkg/memcache"
)

func main() {
	memc := memcache.New()
	defer memc.Close()

	fmt.Println(memc.Get("something"))
	memc.Set("something", 123, 500*time.Millisecond)
	memc.Set("this", 321, 500*time.Millisecond)
	memc.Set("that", 567, 500*time.Millisecond)
	fmt.Println(memc.Get("something"))
	fmt.Println(memc.Get("this"))
	fmt.Println(memc.Get("that"))
	time.Sleep(500 * time.Millisecond)

	fmt.Println(memc.Get("something"))
	fmt.Println(memc.Get("this"))
	fmt.Println(memc.Get("that"))
}
