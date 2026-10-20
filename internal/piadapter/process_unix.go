//go:build !windows

package piadapter

import (
	"errors"
	"os/exec"
	"syscall"
)

// processOwner owns a process group. Descendants that deliberately detach from
// the group are outside this containment boundary, so it is never certified.
type processOwner struct {
	cmd *exec.Cmd
}

func startOwned(cmd *exec.Cmd) (*processOwner, error) {
	if cmd == nil {
		return nil, errors.New("nil command")
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	if cmd.SysProcAttr.Setpgid {
		return nil, errors.New("command already requests a process group")
	}
	cmd.SysProcAttr.Setpgid = true
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &processOwner{cmd: cmd}, nil
}

// Terminate explicitly cancels the owned process group; it installs no timer.
func (p *processOwner) Terminate() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return errors.New("process owner is not started")
	}
	err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func (p *processOwner) Close() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	// Closing ownership alone must not implicitly terminate a POSIX group.
	return nil
}

func (p *processOwner) Certified() bool { return false }
