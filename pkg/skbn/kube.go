package skbn

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nuvo/skbn/pkg/utils"

	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// K8sClient holds a clientset and a config
type K8sClient struct {
	ClientSet *kubernetes.Clientset
	Config    *rest.Config
}

// GetClientToK8s returns a k8sClient
func GetClientToK8s() (*K8sClient, error) {
	var kubeconfig string
	if kubeConfigPath := os.Getenv("KUBECONFIG"); kubeConfigPath != "" {
		kubeconfig = kubeConfigPath // CI process
	} else {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config") // Development environment
	}

	var config *rest.Config

	_, err := os.Stat(kubeconfig)
	if err != nil {
		// In cluster configuration
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	} else {
		// Out of cluster configuration
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	var client = &K8sClient{ClientSet: clientset, Config: config}
	return client, nil
}

// GetListOfFilesFromK8s gets list of files in path from Kubernetes (recursive)
func GetListOfFilesFromK8s(iClient interface{}, path, findType, findName string) ([]string, error) {
	client := *iClient.(*K8sClient)
	pSplit := strings.Split(path, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return nil, err
	}
	namespace, podName, containerName, findPath := initK8sVariables(pSplit)
	command := []string{"find", findPath, "-type", findType, "-name", findName}

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		output := new(bytes.Buffer)
		stderr, err := Exec(client, namespace, podName, containerName, command, nil, output)
		if len(stderr) != 0 {
			if attempt == attempts {
				return nil, fmt.Errorf("STDERR: " + (string)(stderr))
			}
			utils.Sleep(attempt)
			continue
		}
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
			utils.Sleep(attempt)
			continue
		}

		lines := strings.Split(output.String(), "\n")
		var outLines []string
		for _, line := range lines {
			if line != "" {
				outLines = append(outLines, strings.Replace(line, findPath, "", 1))
			}
		}

		return outLines, nil
	}

	return nil, nil
}

// DownloadFromK8s downloads a single file from Kubernetes
func DownloadFromK8s(iClient interface{}, path string, writer io.Writer) error {
	client := *iClient.(*K8sClient)
	pSplit := strings.Split(path, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return err
	}
	namespace, podName, containerName, pathToCopy := initK8sVariables(pSplit)

	isToBeEncrypted, passphrase := isK8sCryptoEnabled(iClient, namespace)

	var command = []string{"cat", pathToCopy}
	if isToBeEncrypted {
		command = []string{
			"gpg",
			"--homedir",
			"/tmp",
			"--batch",
			"--cipher-algo",
			"AES256",
			"--passphrase",
			passphrase,
			"-o", "-",
			"--symmetric",
			pathToCopy,
		}
	}

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		stderr, err := Exec(client, namespace, podName, containerName, command, nil, writer)
		if attempt == attempts {
			if len(stderr) != 0 {
				return fmt.Errorf("STDERR: " + (string)(stderr))
			}
			if err != nil {
				return err
			}
		}
		if err == nil {
			return nil
		}
		utils.Sleep(attempt)
	}

	return nil
}

// Clean downloads a single file from Kubernetes
func Clean(iClient interface{}, path string) error {
	client := *iClient.(*K8sClient)
	pSplit := strings.Split(path, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return err
	}
	namespace, podName, containerName, pathToRemove := initK8sVariables(pSplit)

	command := []string{
		"rm",
		pathToRemove,
	}

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		stderr, err := Exec(client, namespace, podName, containerName, command, nil, nil)
		if attempt == attempts {
			if len(stderr) != 0 {
				return fmt.Errorf("STDERR: " + (string)(stderr))
			}
			if err != nil {
				return err
			}
		}
		if err == nil {
			return nil
		}
		utils.Sleep(attempt)
	}

	return nil
}

// UploadToK8s uploads a single file to Kubernetes
func UploadToK8s(iClient interface{}, toPath, fromPath string, reader io.Reader) error {
	client := *iClient.(*K8sClient)
	pSplit := strings.Split(toPath, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return err
	}
	if len(pSplit) == 3 {
		_, fileName := filepath.Split(fromPath)
		pSplit = append(pSplit, fileName)
	}
	namespace, podName, containerName, pathToCopy := initK8sVariables(pSplit)

	isToBeDecrypted, passphrase := isK8sCryptoEnabled(iClient, namespace)

	attempts := 3
	attempt := 0
	retryWithoutDecryption := false
	for attempt < attempts {
		attempt++
		dir := filepath.Dir(pathToCopy)

		command := []string{"mkdir", "-p", dir}
		stderr, err := Exec(client, namespace, podName, containerName, command, nil, nil)

		if len(stderr) != 0 {
			if attempt == attempts {
				return fmt.Errorf("STDERR: " + (string)(stderr))
			}
			utils.Sleep(attempt)
			continue
		}
		if err != nil {
			if attempt == attempts {
				return err
			}
			utils.Sleep(attempt)
			continue
		}

		command = []string{"touch", pathToCopy}
		stderr, err = Exec(client, namespace, podName, containerName, command, nil, nil)

		if len(stderr) != 0 {
			if attempt == attempts {
				return fmt.Errorf("STDERR: " + (string)(stderr))
			}
			utils.Sleep(attempt)
			continue
		}
		if err != nil {
			if attempt == attempts {
				return err
			}
			utils.Sleep(attempt)
			continue
		}

		command = []string{"cp", "/dev/stdin", pathToCopy}
		if isToBeDecrypted && !retryWithoutDecryption {
			command = []string{
				"gpg",
				"--homedir", "/tmp",
				"--batch",
				"--yes",
				"--cipher-algo", "AES256",
				"--passphrase", passphrase,
				"--output", pathToCopy,
				"--decrypt", "-",
			}
		}
		stderr, err = Exec(client, namespace, podName, containerName, command, readerWrapper{reader}, nil)

		if len(stderr) != 0 && !isToBeDecrypted {
			if attempt == attempts {
				return fmt.Errorf("STDERR: " + (string)(stderr))
			}
			utils.Sleep(attempt)
			continue
		}
		if len(stderr) != 0 &&
			isToBeDecrypted &&
			!retryWithoutDecryption &&
			strings.Contains((string)(stderr), "gpg: no valid OpenPGP data found.") {
			// Perform a retry without decryption (if this attempt was made with decryption)
			// Don't count this attempt
			attempt--
			// And all re-attempts should be made without decryption
			retryWithoutDecryption = true
			log.Printf("Retry to upload to K8s without decryption")
			continue
		}
		if err != nil {
			if attempt == attempts {
				return err
			}
			utils.Sleep(attempt)
			continue
		}
		return nil
	}
	return nil
}

// Should files be en-/decrypted when transfered from/to K8s
func isK8sCryptoEnabled(iClient interface{}, namespace string) (bool, string) {
	client := *iClient.(*K8sClient)
	// TODO Should consider some ENV to override the use of dis-/enable encryption
	res, err := client.ClientSet.CoreV1().Secrets(namespace).Get("backup-secret", meta_v1.GetOptions{})
	return err == nil, string(res.Data["passphrase"])
}

type readerWrapper struct {
	reader io.Reader
}

func (r readerWrapper) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

// Exec executes a command in a given container
func Exec(client K8sClient, namespace, podName, containerName string, command []string, stdin io.Reader, stdout io.Writer) ([]byte, error) {
	restClient := client.ClientSet.CoreV1().RESTClient()

	req := restClient.Post().
		Namespace(namespace).
		Resource("pods").
		Name(podName).
		SubResource("exec").
		Param("container", containerName).
		Param("stdin", strconv.FormatBool(stdin != nil)).
		Param("stdout", strconv.FormatBool(stdout != nil)).
		Param("stderr", "true")

	for _, command := range command {
		req.Param("command", command)
	}

	executor, err := remotecommand.NewSPDYExecutor(client.Config, http.MethodPost, req.URL())
	if err != nil {
		return nil, fmt.Errorf("error while creating Executor: %v", err)
	}

	if stdin != nil {
		os.Stdout.Sync()
	}
	os.Stderr.Sync()

	var stderr bytes.Buffer
	err = executor.Stream(remotecommand.StreamOptions{
		Stdin:             stdin,
		Stdout:            stdout,
		Stderr:            &stderr,
		Tty:               false,
		TerminalSizeQueue: nil,
	})

	if err != nil {
		return stderr.Bytes(), fmt.Errorf("error in Stream: %v", err)
	}

	return stderr.Bytes(), nil
}

func validateK8sPath(pathSplit []string) error {
	if len(pathSplit) >= 3 {
		return nil
	}
	return fmt.Errorf("illegal path: %s", filepath.Join(pathSplit...))
}

func initK8sVariables(split []string) (string, string, string, string) {
	namespace := split[0]
	pod := split[1]
	container := split[2]
	path := getAbsPath(split[3:]...)

	return namespace, pod, container, path
}

func getAbsPath(path ...string) string {
	return filepath.Join("/", filepath.Join(path...))
}
