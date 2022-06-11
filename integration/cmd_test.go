package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"testing"
)

const (
	BIN = "ddbrew"
)

type ConsoleResult struct {
	stdout   string
	stderrRe *regexp.Regexp
	exitCode int
}

func init() {
	fmt.Fprintln(os.Stderr, "# NOTE: Please check README.md before running the integration test")
	out, err := exec.Command(BIN, "--version").Output()

	if err != nil {
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "testing %s\n", string(out))
}

func TestCmd_BackupNG_NotAWSAssumeRole(t *testing.T) {
	ctx := context.Background()
	env := []string{"HOME=" + os.Getenv("HOME")}

	cmd, _, _ := commandContext(ctx, env, "backup", ONDEMAND_TABLE_NAME)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()

	want := ConsoleResult{
		exitCode: 1,
	}

	if cmd.ProcessState.ExitCode() != want.exitCode {
		t.Fatalf("return code %d, want %d", cmd.ProcessState.ExitCode(), want.exitCode)
	}
}

func TestCmd_BackupOK(t *testing.T) {
	ctx := context.Background()
	env := []string{
		"HOME=" + os.Getenv("HOME"),
		"AWS_ACCESS_KEY_ID=" + os.Getenv("AWS_ACCESS_KEY_ID"),
		"AWS_SECRET_ACCESS_KEY=" + os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"AWS_SESSION_TOKEN=" + os.Getenv("AWS_SESSION_TOKEN"),
		"AWS_SECURITY_TOKEN=" + os.Getenv("AWS_SECURITY_TOKEN"),
	}

	t.Run("backup success", func(t *testing.T) {
		err := createBackupTestData(ONDEMAND_TABLE_NAME)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err = cleanBackupTestData(ONDEMAND_TABLE_NAME); err != nil {
				t.Fatal(err)
			}
		})

		cmd, _, stderr := commandContext(ctx, env, "backup", ONDEMAND_TABLE_NAME)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		cmd.Wait()

		stderrRe := regexp.MustCompile(`created ~(.*?)backup_DdbrewPrimaryOnDemand_\d{8}-\d{6}.jsonl\n\rscaned records: 1000backuped\n`)

		want := ConsoleResult{
			exitCode: 0,
			stderrRe: stderrRe,
		}

		if cmd.ProcessState.ExitCode() != want.exitCode {
			t.Fatalf("return code %d, want %d", cmd.ProcessState.ExitCode(), want.exitCode)
		}

		if got := len(want.stderrRe.FindAllString(stderr.String(), -1)); got != 1 {
			t.Fatalf("return code %d, want %d", got, 1)
		}
	})
}

func commandContext(ctx context.Context, env []string, arg ...string) (cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
	cmd = exec.CommandContext(ctx, BIN, arg...)
	cmd.Env = env

	var outBuf, errBuf bytes.Buffer

	cmd.Stdout, cmd.Stderr = &outBuf, &errBuf

	cmd.Stdout = io.MultiWriter(&outBuf, os.Stdout)
	cmd.Stderr = io.MultiWriter(&errBuf, os.Stderr)

	return cmd, &outBuf, &errBuf
}
