package skbn

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ncw/swift"
	"github.com/nuvo/skbn/pkg/utils"
)

// GetClientToSwift authenticates to swift and returns client
func GetClientToSwift(path string) (*swift.Connection, error) {
	pSplit := strings.Split(path, "/")
	container, _ := initSwiftVariables(pSplit)

	c, err := getNewConnection()
	if err != nil {
		return nil, err
	}

	err = c.Authenticate()
	if err != nil {
		return nil, fmt.Errorf("Authentication failed with the Swift client")
	}

	opts := swift.ContainersOpts{}
	lc, err := c.ContainerNamesAll(&opts)
	if err != nil {
		return nil, fmt.Errorf("Can't fetch container list for container check")
	}

	if !containerCheck(lc, container) {
		return nil, fmt.Errorf("Swift storage container doesn't exist")
	}

	return c, nil
}

// DownloadFromSwift downloads a single file from Swift
func DownloadFromSwift(iClient interface{}, path string, writer io.Writer) error {
	conn := iClient.(*swift.Connection)

	pSplit := strings.Split(path, "/")
	if err := validateSwiftPath(pSplit); err != nil {
		return err
	}

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		container, swiftPath := initSwiftVariables(pSplit)

		_, err := conn.ObjectGet(container, swiftPath, writer, true, nil)
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

// UploadToSwift uploads a single file to Swift Storage
func UploadToSwift(iClient interface{}, toPath, fromPath string, reader io.Reader) error {
	conn := iClient.(*swift.Connection)

	pSplit := strings.Split(toPath, "/")
	if err := validateSwiftPath(pSplit); err != nil {
		return err
	}

	if len(pSplit) == 1 {
		_, fn := filepath.Split(fromPath)
		pSplit = append(pSplit, fn)
	}

	container, swiftPath := initSwiftVariables(pSplit)

	_, err := conn.ObjectPut(container, swiftPath, reader, true, "", "", nil)
	if err != nil {
		return err
	}

	return nil
}

func initSwiftVariables(split []string) (string, string) {
	container := split[0]
	path := filepath.Join(split[1:]...)

	return container, path
}

func containerCheck(list []string, containerName string) bool {
	exists := false
	for _, v := range list {
		if containerName == v {
			exists = true
		}
	}
	return exists
}

func validateSwiftPath(pathSplit []string) error {
	if len(pathSplit) >= 1 {
		return nil
	}
	return fmt.Errorf("illegal path: %s", filepath.Join(pathSplit...))
}

func getNewConnection() (*swift.Connection, error) {

	user := os.Getenv("SWIFT_API_USER")
	if user == "" {
		return nil, fmt.Errorf("Missing SWIFT_API_USER")
	}
	key := os.Getenv("SWIFT_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("Missing SWIFT_API_KEY")
	}
	authURL := os.Getenv("SWIFT_AUTH_URL")
	if authURL == "" {
		return nil, fmt.Errorf("Missing SWIFT_AUTH_URL")
	}
	tenantName := os.Getenv("SWIFT_TENANT")
	if tenantName == "" {
		return nil, fmt.Errorf("Missing SWIFT_TENANT")
	}
	domainName := os.Getenv("SWIFT_API_DOMAIN")
	if domainName == "" {
		return nil, fmt.Errorf("Missing SWIFT_API_DOMAIN")
	}

	c := &swift.Connection{
		UserName: user,
		ApiKey:   key,
		AuthUrl:  authURL,
		Domain:   domainName,
		Tenant:   tenantName,
	}

	return c, nil

}
