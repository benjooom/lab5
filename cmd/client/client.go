package main

import (
	"context"
	"encoding/json"
	"flag"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"cs426.yale.edu/lab4/kv"
	"cs426.yale.edu/lab4/logging"
)

// Simple CLI for interacting with a KV cluster. You can get, set, or delete values using the Kv API.
// You must pass in a shardmap as a JSON file with --shardmap=path/to/shardmap.json
//
// Examples:
//   - go run cmd/client/client.go --shardmap=shardmaps/test-1.json set abc 123 5000  # sets "abc" to "123" with TTL of 5s (5000ms)
//   - go run cmd/client/client.go --shardmap=shardmaps/test-1.json get abc           # retrieves value for key "abc" (should be "123")
//   - go run cmd/client/client.go --shardmap=shardmaps/test-1.json delete abc        # removes value at "abc"

var (
	shardMapFile = flag.String("shardmap", "", "Path to a JSON file which describes the shard map")
)

func usage() {
	logrus.Fatal("Usage: client.go [get|set|delete|getList|setList|getSet|setSet] key [value] [ttl]")
}

func main() {
	flag.Parse()
	logging.InitLogging()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	fileSm, err := kv.WatchShardMapFile(*shardMapFile)
	if err != nil {
		logrus.Fatal(err)
	}

	clientPool := kv.MakeClientPool(&fileSm.ShardMap)

	client := kv.MakeKv(&fileSm.ShardMap, &clientPool)

	subcommand := args[0]
	key := args[1]

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	switch subcommand {
	case "get":
		value, isSet, err := client.Get(ctx, key)
		if err != nil {
			logrus.WithField("key", key).Errorf("error getting value for key: %q", err)
		} else if !isSet {
			logrus.WithField("key", key).Info("no value set for key")
		} else {
			println(value)
		}
	case "set":
		if len(args) < 4 {
			usage()
		}
		value := args[2]
		ttlMs, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			logrus.Fatalf("expected int value for ttlMs: %q", err)
		}
		err = client.Set(ctx, key, value, time.Duration(ttlMs)*time.Millisecond)
		if err != nil {
			logrus.WithField("key", key).Errorf("error setting value: %q", err)
		}
	case "delete":
		err := client.Delete(ctx, key)
		if err != nil {
			logrus.WithField("key", key).Errorf("error deleting value for key: %q", err)
		}
	case "getList":
		value, err := client.GetList(ctx, key)
		if err != nil {
			logrus.WithField("key", key).Errorf("error getting value for key: %q", err)
		} else {
			// Convert []string to JSON string
			jsonVal, err := json.Marshal(value)
			if err != nil {
				logrus.Fatalf("expected list value for value: %q", err)
			}
			println(string(jsonVal))
		}
	case "setList":
		if len(args) < 4 {
			usage()
		}
		value := args[2]
		// Convert value of type "['hi','hello']" to []string
		convertedVal := make([]string, 0)
		err := json.Unmarshal([]byte(value), &convertedVal)
		if err != nil {
			logrus.Fatalf("expected list value for value: %q", err)
		}

		ttlMs, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			logrus.Fatalf("expected int value for ttlMs: %q", err)
		}
		client.SetList(ctx, key, convertedVal, time.Duration(ttlMs)*time.Millisecond)

	case "getSet":
		value, err := client.GetSet(ctx, key)
		if err != nil {
			logrus.WithField("key", key).Errorf("error getting value for key: %q", err)
		} else {
			// Convert []string to JSON string
			jsonVal, err := json.Marshal(value)
			if err != nil {
				logrus.Fatalf("expected list value for value: %q", err)
			}
			println(string(jsonVal))
		}
	case "setSet":
		if len(args) < 4 {
			usage()
		}
		value := args[2]
		// Convert value of type "['hi','hello']" to []string
		convertedVal := make([]string, 0)
		err := json.Unmarshal([]byte(value), &convertedVal)
		if err != nil {
			logrus.Fatalf("expected list value for value: %q", err)
		}

		ttlMs, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			logrus.Fatalf("expected int value for ttlMs: %q", err)
		}
		client.SetSet(ctx, key, convertedVal, time.Duration(ttlMs)*time.Millisecond)
	default:
		usage()
	}
	cancel()
}
