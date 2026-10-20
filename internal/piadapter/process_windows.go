//go:build windows

package piadapter

import (
	"fmt"
	"golang.org/x/sys/windows"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

// processOwner assigns a suspended process before any child code can run.
// Breakaway is not enabled, so subsequently created descendants inherit the job.
type processOwner struct {
	job  windows.Handle
	once sync.Once
	err  error
}

func startOwned(cmd *exec.Cmd) (*processOwner, error) {
	job, e := windows.CreateJobObject(nil, nil)
	if e != nil {
		return nil, e
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, e = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); e != nil {
		windows.CloseHandle(job)
		return nil, e
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	if e = cmd.Start(); e != nil {
		windows.CloseHandle(job)
		return nil, e
	}
	fail := func(cause error) (*processOwner, error) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		windows.CloseHandle(job)
		return nil, cause
	}
	process, e := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if e != nil {
		return fail(e)
	}
	e = windows.AssignProcessToJobObject(job, process)
	windows.CloseHandle(process)
	if e != nil {
		return fail(fmt.Errorf("assign suspended Pi to job: %w", e))
	}
	snapshot, e := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if e != nil {
		return fail(e)
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	found := false
	for e = windows.Thread32First(snapshot, &entry); e == nil; e = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != uint32(cmd.Process.Pid) {
			continue
		}
		thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if err != nil {
			return fail(err)
		}
		_, err = windows.ResumeThread(thread)
		windows.CloseHandle(thread)
		if err != nil {
			return fail(err)
		}
		found = true
		break
	}
	if !found {
		return fail(fmt.Errorf("suspended Pi primary thread not found"))
	}
	return &processOwner{job: job}, nil
}
func (p *processOwner) Terminate() error { return windows.TerminateJobObject(p.job, 1) }
func (p *processOwner) Close() error {
	p.once.Do(func() { p.err = windows.CloseHandle(p.job) })
	return p.err
}
func (p *processOwner) Certified() bool { return p != nil && p.job != 0 }
