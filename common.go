package arpc

import (
	"log"
	"strings"

	uuid "github.com/satori/go.uuid"
)

// UUID generates UUID V4 string.
func UUID(withDash bool) string {
	id := uuid.NewV4().String()
	if withDash {
		return id
	}
	return strings.ReplaceAll(id, "-", "")
}

// failOnError log fatal the error, please use this function wisely.
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
