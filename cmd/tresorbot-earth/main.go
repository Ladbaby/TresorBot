package main

import (
	"flag"
	"fmt"
	"os"
	"tresorbot/internal/config"
)

var version = "dev"

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(a []string) error {
	f := flag.NewFlagSet("tresorbot-earth", flag.ContinueOnError)
	p := f.String("config", "", "YAML config path")
	c := f.Bool("check-config", false, "validate config and exit")
	v := f.Bool("version", false, "print version and exit")
	if e := f.Parse(a); e != nil {
		return e
	}
	if *v {
		fmt.Println(version)
		return nil
	}
	if *p == "" {
		return fmt.Errorf("--config is required")
	}
	if _, e := config.Load(*p, "earth"); e != nil {
		return e
	}
	if *c {
		fmt.Println("config ok")
		return nil
	}
	return fmt.Errorf("earth runner execution is not implemented yet (planned for B3)")
}
