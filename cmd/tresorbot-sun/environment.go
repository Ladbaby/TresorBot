package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"tresorbot/internal/adapters"
	"tresorbot/internal/environments"
)

// runEnvironment implements `tresorbot-sun environment <command>`.
//
// Installing an input is a CLI action rather than a dashboard one for the same
// reason it is on Earth: an input is executable code, so it arrives as a
// digest-pinned archive fetched from a URL the operator typed. A web form that
// installs code is a remote-code-execution surface with a login page in front of
// it, and the operator already has shell access to the host running Sun.
func runEnvironment(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: tresorbot-sun environment <install|list|remove>")
	}
	dir, err := environments.Dir()
	if err != nil {
		return err
	}
	switch args[0] {
	case "install":
		// The flag package stops at the first positional argument, but
		// `install <url> --sha256 <digest>` is the natural order to type, so flags
		// are pulled out before the positional arguments are checked.
		digest, positional, err := splitFlags(args[1:], "sha256")
		if err != nil {
			return err
		}
		if len(positional) != 1 {
			return fmt.Errorf("usage: tresorbot-sun environment install <https-url> [--sha256 <digest>]")
		}
		installed, err := adapters.Install(context.Background(), adapters.InstallInput{
			URL: positional[0], SHA256: digest, Dir: dir, ManifestName: environments.ManifestName,
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
		fmt.Println("Restart Sun, or wait for it to reconcile, then add an instance in the dashboard.")
		return nil

	case "list":
		list, err := adapters.ListWithManifest(dir, environments.ManifestName)
		if err != nil {
			return err
		}
		if len(list) == 0 {
			fmt.Printf("no inputs installed in %s\n", dir)
			fmt.Println("install one with: tresorbot-sun environment install <https-url> [--sha256 <digest>]")
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
			return fmt.Errorf("usage: tresorbot-sun environment remove <name>")
		}
		// Removing an installed input does not delete the instances that use it:
		// their settings and cursors are the owner's data, and an instance whose
		// input is missing is reported as misconfigured rather than silently
		// dropped. Reinstalling the input makes it work again unchanged.
		if err := adapters.RemoveWithManifest(dir, args[1], environments.ManifestName); err != nil {
			return err
		}
		fmt.Printf("removed %s\n", args[1])
		return nil
	}
	return fmt.Errorf("usage: tresorbot-sun environment <install|list|remove>")
}

// splitFlags pulls one valued flag out of an argument list, leaving positionals.
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
