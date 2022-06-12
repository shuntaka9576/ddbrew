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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	util "github.com/shuntaka9576/ddbrew/internal/testingutil"
)

const (
	BIN = "ddbrew"
)

var tableName string

func init() {
	fmt.Fprintln(os.Stderr, "# NOTE: Please check README.md before running the integration test")
	out, err := exec.Command(BIN, "--version").Output()

	if err != nil {
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "testing %s\n", string(out))
}

func beforeAll() (err error) {
	tableName, err = util.CreateTable(&util.TableOption{Mode: types.BillingModePayPerRequest, Secondary: false})
	if err != nil {
		return err
	}

	return nil
}

func afterAll() error {
	err := util.DeleteTable()
	if err != nil {
		return err
	}

	return nil
}

func TestMain(m *testing.M) {
	err := beforeAll()
	if err != nil {
		fmt.Fprintf(os.Stderr, "beforeAll error: %s\n", err)

		os.Exit(1)
	}

	defer func() {
		err := afterAll()
		if err != nil {
			fmt.Fprintf(os.Stderr, "afterAll error: %s\n", err)

			os.Exit(1)
		}
	}()

	m.Run()
}

func TestCmd_BackupNG_NotAWSAssumeRole(t *testing.T) {
	ctx := context.Background()
	env := []string{"HOME=" + os.Getenv("HOME")}

	cmd, _, _ := commandContext(ctx, env, "backup", tableName)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()

	want := struct{ exitCode int }{
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
		err := util.CreateBackupTestData()
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			if err = util.CleanBackupTestData(); err != nil {
				t.Fatal(err)
			}
		})

		cmd, _, stderr := commandContext(ctx, env, "backup", tableName)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		cmd.Wait()

		want := struct {
			exitCode     int
			stderrRegexp *regexp.Regexp
		}{
			exitCode:     0,
			stderrRegexp: regexp.MustCompile(`created ~(.*?)backup_DdbrewPrimaryOnDemand_\d{8}-\d{6}.jsonl\n\rscaned records: 1000backuped\n`),
		}

		if cmd.ProcessState.ExitCode() != want.exitCode {
			t.Fatalf("return code %d, want %d", cmd.ProcessState.ExitCode(), want.exitCode)
		}

		if got := len(want.stderrRegexp.FindAllString(stderr.String(), -1)); got != 1 {
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
