package redispubsub_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	log "github.com/sirupsen/logrus"
)

var redisCli *redis.Client

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "redis", // image name
		Tag:        "7",     // version tag
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	resource.Expire(120) // Tell docker to hard kill the container in 120 seconds

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 120 * time.Second

	hostAndPort := resource.GetHostPort("6379/tcp")
	databaseUrl := fmt.Sprintf("redis://%s/0", hostAndPort)

	os.Setenv("REDIS_URL", databaseUrl)

	if err = pool.Retry(func() error {
		opt, err := redis.ParseURL(databaseUrl)
		if err != nil {
			return err
		}
		rdb := redis.NewClient(opt)
		redisCli = rdb
		_, err = rdb.Ping(context.Background()).Result()
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	// Run tests
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}
