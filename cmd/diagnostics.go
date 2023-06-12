package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/apex/log"
	"github.com/docker/docker/pkg/parsers/kernel"
	"github.com/docker/docker/pkg/parsers/operatingsystem"
	"github.com/goccy/go-json"
	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/loggers/cli"
	"github.com/kubectyl/kuber/system"
	"github.com/spf13/cobra"

	"github.com/olekukonko/tablewriter"
)

const (
	DefaultPastebinUrl = "https://pb.kubectyl.org"
	DefaultLogLines    = 200
)

var diagnosticsArgs struct {
	IncludeEndpoints   bool
	IncludeLogs        bool
	ReviewBeforeUpload bool
	PastebinURL        string
	LogLines           int
}

func newDiagnosticsCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "diagnostics",
		Short: "Collect and report information about this Kuber instance to assist in debugging.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initConfig()
			log.SetHandler(cli.Default)
		},
		Run: diagnosticsCmdRun,
	}

	command.Flags().StringVar(&diagnosticsArgs.PastebinURL, "hastebin-url", DefaultPastebinUrl, "the url of the hastebin instance to use")
	command.Flags().IntVar(&diagnosticsArgs.LogLines, "log-lines", DefaultLogLines, "the number of log lines to include in the report")

	return command
}

// diagnosticsCmdRun collects diagnostics about kuber, its configuration and the node.
// We collect:
// - kuber and docker versions
// - relevant parts of daemon configuration
// - the docker debug output
// - running docker containers
// - logs
func diagnosticsCmdRun(*cobra.Command, []string) {
	questions := []*survey.Question{
		{
			Name:   "IncludeEndpoints",
			Prompt: &survey.Confirm{Message: "Do you want to include endpoints (i.e. the FQDN/IP of your panel)?", Default: false},
		},
		{
			Name:   "IncludeLogs",
			Prompt: &survey.Confirm{Message: "Do you want to include the latest logs?", Default: true},
		},
		{
			Name: "ReviewBeforeUpload",
			Prompt: &survey.Confirm{
				Message: "Do you want to review the collected data before uploading to " + diagnosticsArgs.PastebinURL + "?",
				Help:    "The data, especially the logs, might contain sensitive information, so you should review it. You will be asked again if you want to upload.",
				Default: true,
			},
		},
	}
	if err := survey.Ask(questions, &diagnosticsArgs); err != nil {
		if err == terminal.InterruptErr {
			return
		}
		panic(err)
	}

	output := &strings.Builder{}
	fmt.Fprintln(output, "Kubectyl Kuber - Diagnostics Report")

	v, _ := kernel.GetKernelVersion()
	osv, _ := operatingsystem.GetOperatingSystem()

	cfg := config.Get()
	data := [][]string{
		{"Kuber", system.Version},
		{"Kernel", v.String()},
		{"OS", osv},

		{"Panel Location", redact(cfg.PanelLocation)},
		{"Internal Webserver", fmt.Sprintf("%s:%d", redact(cfg.Api.Host), cfg.Api.Port)},
		{"SSL Enabled", fmt.Sprintf("%t", cfg.Api.Ssl.Enabled)},
		{"SSL Certificate", redact(cfg.Api.Ssl.CertificateFile)},
		{"SSL Key", redact(cfg.Api.Ssl.KeyFile)},

		{"Root Directory", cfg.System.RootDirectory},
		{"Logs Directory", cfg.System.LogDirectory},
		{"Data Directory", cfg.System.Data},
		{"Archive Directory", cfg.System.ArchiveDirectory},

		{"Server Time", time.Now().Format(time.RFC1123Z)},
		{"Debug Mode", fmt.Sprintf("%t", cfg.Debug)},
	}

	table := tablewriter.NewWriter(output)
	table.SetHeader([]string{"Variable", "Value"})
	table.SetRowLine(true)
	table.AppendBulk(data)
	table.Render()

	printHeader(output, "Latest Kuber Logs")
	if diagnosticsArgs.IncludeLogs {
		p := "/var/log/kubectyl/kuber.log"
		if cfg != nil {
			p = path.Join(cfg.System.LogDirectory, "kuber.log")
		}
		if c, err := exec.Command("tail", "-n", strconv.Itoa(diagnosticsArgs.LogLines), p).Output(); err != nil {
			fmt.Fprintln(output, "No logs found or an error occurred.")
		} else {
			fmt.Fprintf(output, "%s\n", string(c))
		}
	} else {
		fmt.Fprintln(output, "Logs redacted.")
	}

	if !diagnosticsArgs.IncludeEndpoints {
		s := output.String()
		output.Reset()
		s = strings.ReplaceAll(s, cfg.PanelLocation, "{redacted}")
		s = strings.ReplaceAll(s, cfg.Api.Host, "{redacted}")
		s = strings.ReplaceAll(s, cfg.Api.Ssl.CertificateFile, "{redacted}")
		s = strings.ReplaceAll(s, cfg.Api.Ssl.KeyFile, "{redacted}")
		output.WriteString(s)
	}

	fmt.Println("\n---------------  generated report  ---------------")
	fmt.Println(output.String())
	fmt.Print("---------------   end of report    ---------------\n\n")

	upload := !diagnosticsArgs.ReviewBeforeUpload
	if !upload {
		survey.AskOne(&survey.Confirm{Message: "Upload to " + diagnosticsArgs.PastebinURL + "?", Default: false}, &upload)
	}
	if upload {
		passwordFunc := func(length int) string {
			charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
			password := make([]byte, length)

			for i := 0; i < length; i++ {
				password[i] = charset[rand.Intn(len(charset))]
			}

			return string(password)
		}
		rand.Seed(time.Now().UnixNano())

		password := passwordFunc(8)
		result, err := uploadToPastebin(diagnosticsArgs.PastebinURL, output.String(), password)
		if err == nil {
			seconds, err := strconv.Atoi(fmt.Sprintf("%v", result["expire"]))
			if err != nil {
				return
			}

			expireTime := fmt.Sprintf("%d hours, %d minutes, %d seconds", seconds/3600, (seconds%3600)/60, seconds%60)

			fmt.Println("Your report is available here:", result["url"])
			fmt.Println("Will expire in", expireTime)
			fmt.Printf("You can edit your pastebin here: %s\n", result["admin"])
		}
	}
}

// func getDockerInfo() (types.Version, types.Info, error) {
// 	client, err := environment.Docker()
// 	if err != nil {
// 		return types.Version{}, types.Info{}, err
// 	}
// 	dockerVersion, err := client.ServerVersion(context.Background())
// 	if err != nil {
// 		return types.Version{}, types.Info{}, err
// 	}
// 	dockerInfo, err := client.Info(context.Background())
// 	if err != nil {
// 		return types.Version{}, types.Info{}, err
// 	}
// 	return dockerVersion, dockerInfo, nil
// }

func uploadToPastebin(pbURL, content, password string) (map[string]interface{}, error) {
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	writer.WriteField("c", content)
	writer.WriteField("e", "300")
	writer.WriteField("s", password)
	writer.Close()

	u, err := url.Parse(pbURL)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil || res.StatusCode != 200 {
		fmt.Println("Failed to upload report to", u.String(), err)
		return nil, err
	}

	pres := make(map[string]interface{})
	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Failed to parse response.", err)
		return nil, err
	}
	json.Unmarshal(body, &pres)
	if key, ok := pres["url"].(string); ok {
		u.Path = key
		return pres, nil
	}

	return nil, errors.New("failed to find key in response")
}

func redact(s string) string {
	if !diagnosticsArgs.IncludeEndpoints {
		return "{redacted}"
	}
	return s
}

func printHeader(w io.Writer, title string) {
	fmt.Fprintln(w, "\n|\n|", title)
	fmt.Fprintln(w, "| ------------------------------")
}
