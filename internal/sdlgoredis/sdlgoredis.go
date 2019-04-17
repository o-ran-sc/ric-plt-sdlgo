package sdlgoredis

import (
	"errors"
	"fmt"
	"os"

	"github.com/go-redis/redis"
)

type DB struct {
	client       *redis.Client
	redisModules bool
}

func Create() *DB {
	hostname := os.Getenv("DBAAS_SERVICE_HOST")
	if hostname == "" {
		hostname = "localhost"
	}
	port := os.Getenv("DBAAS_SERVICE_PORT")
	if port == "" {
		port = "6379"
	}
	redisAddress := hostname + ":" + port
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
		PoolSize: 20,
	})

	db := DB{
		client:       client,
		redisModules: true,
	}

	commands, err := db.client.Command().Result()
	if err == nil {
		redisModuleCommands := []string{"setie", "delie"}
		for _, v := range redisModuleCommands {
			_, ok := commands[v]
			if !ok {
				db.redisModules = false
			}
		}
	} else {
		fmt.Println(err)
	}
	return &db
}

func (db *DB) Close() error {
	return db.client.Close()
}

func (db *DB) MSet(pairs ...interface{}) error {
	return db.client.MSet(pairs...).Err()
}

func (db *DB) MGet(keys []string) ([]interface{}, error) {
	val, err := db.client.MGet(keys...).Result()
	return val, err
}

func (db *DB) Del(keys []string) error {
	_, err := db.client.Del(keys...).Result()
	return err
}

func (db *DB) Keys(pattern string) ([]string, error) {
	val, err := db.client.Keys(pattern).Result()
	return val, err
}

func (db *DB) SetIE(key string, oldData, newData interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command")
	}

	result, err := db.client.Do("SETIE", key, newData, oldData).Result()
	if err != nil {
		return false, err
	}
	if result == "OK" {
		return true, nil
	} else {
		return false, nil
	}
}

func (db *DB) SetNX(key string, data interface{}) (bool, error) {
	result, err := db.client.SetNX(key, data, 0).Result()
	return result, err
}

func (db *DB) DelIE(key string, data interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command")
	}
	result, err := db.client.Do("DELIE", key, data).Result()
	if err != nil {
		return false, err
	}
	if result == "1" {
		return true, nil
	}
	return false, nil
}
