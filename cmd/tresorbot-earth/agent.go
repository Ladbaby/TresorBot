package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"tresorbot/internal/adapters"
	"tresorbot/internal/config"
)

// runAgent implements `tresorbot-earth agent <command>`.
//
// Installation is a separate step from running Earth because adapters are
// plugins: a fresh Earth starts fine with none installed, and an operator adds
// only the agents they intend to use. That is why there is no adapter catalogue
// in earth.yaml — the catalogue is whatever is installed on disk.
func runAgent(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: tresorbot-earth agent <install|list|remove>")
	}
	dir, err := config.AgentsDir()
	if err != nil {
		return err
	}
	switch args[0] {
	case "install":
		// The flag package stops at the first positional argument, but
		// `install <url> --sha256 <digest>` is the natural order to type, so
		// flags are pulled out before the positional arguments are checked.
		digest, positional, err := splitFlags(args[1:], "sha256")
		if err != nil {
			return err
		}
		if len(positional) != 1 {
			return fmt.Errorf("usage: tresorbot-earth agent install <https-url> [--sha256 <digest>]")
		}
		installed, err := adapters.Install(context.Background(), adapters.InstallInput{
			URL: positional[0], SHA256: digest, Dir: dir,
		})
		if err != nil {
			return err
		}
		fmt.Printf("installed %s %s\n", installed.Name, installed.Version)
		if installed.DisplayName != "" {
			fmt.Printf("  %s\n", installed.DisplayName)
		}
		fmt.Printf("  path:   %s\n", installed.Path)
		fmt.Printf("  sha256: %s\n", installed.Digest)
		if installed.Verified {
			fmt.Println("  verified against the digest you supplied")
		} else {
			// Not a failure, but the operator should know what was and was not
			// checked. Pin the digest on the next install to make it verifiable.
			fmt.Println("  NOT verified: no --sha256 supplied, so the download was not checked against a known digest")
		}
		fmt.Println("Restart Earth, or wait for its watcher to pick it up, then select it on a bot.")
		return nil

	case "list":
		list, err := adapters.List(dir)
		if err != nil {
			return err
		}
		if len(list) == 0 {
			fmt.Printf("no adapters installed in %s\n", dir)
			fmt.Println("install one with: tresorbot-earth agent install <https-url> [--sha256 <digest>]")
			return nil
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tVERSION\tDESCRIPTION")
		for _, a := range list {
			fmt.Fprintf(w, "%s\t%s\t%s\n", a.Name, a.Version, a.DisplayName)
		}
		return w.Flush()

	case "remove":
		if len(args) != 2 {
			return fmt.Errorf("usage: tresorbot-earth agent remove <name>")
		}
		if err := adapters.Remove(dir, args[1]); err != nil {
			return err
		}
		fmt.Printf("removed %s\n", args[1])
		return nil

	default:
		return fmt.Errorf("unknown agent command %q", args[0])
	}
}

// splitFlags extracts `--name value` and `--name=value` pairs wherever they
// appear, returning the named flag's value and the remaining positional
// arguments.
func splitFlags(args []string, name string) (string, []string, error) {
	value := ""
	positional := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--"+name:
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("--%s requires a value", name)
			}
			value = args[i+1]
			i++
		case strings.HasPrefix(arg, "--"+name+"="):
			value = strings.TrimPrefix(arg, "--"+name+"=")
		case strings.HasPrefix(arg, "-"):
			return "", nil, fmt.Errorf("unknown flag %q", arg)
		default:
			positional = append(positional, arg)
		}
	}
	return value, positional, nil
}
