package redis

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/simar7/gokv/util"

	"github.com/gomodule/redigo/redis"
	"github.com/simar7/gokv/types"
)

var (
	ErrInvalidAddress  = errors.New("invalid redis address specified")
	ErrRedisInitFailed = errors.New("redis initialization failed")
	ErrKeyNotFound     = errors.New("key not found")
)

type Options struct {
	MaxIdleConnections   int
	MaxActiveConnections int
	Network              string
	Address              string
}

var DefaultOptions = Options{
	MaxIdleConnections:   80,
	MaxActiveConnections: 10000,
	Network:              "tcp",
}

type Store struct {
	p *redis.Pool
}

func (s Store) ping() error {
	c := s.p.Get()
	defer c.Close()

	_, err := c.Do("PING")
	if err != nil {
		return err
	}

	return nil
}

func NewStore(options Options) (Store, error) {
	if options.Address == "" {
		return Store{}, ErrInvalidAddress
	}

	if options.MaxActiveConnections == 0 {
		options.MaxActiveConnections = DefaultOptions.MaxActiveConnections
	}

	if options.MaxIdleConnections == 0 {
		options.MaxIdleConnections = DefaultOptions.MaxIdleConnections
	}

	if options.Network == "" {
		options.Network = DefaultOptions.Network
	}

	s := Store{
		p: &redis.Pool{
			MaxIdle:   options.MaxIdleConnections,
			MaxActive: options.MaxActiveConnections,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial(options.Network, options.Address)
				if err != nil {
					return nil, fmt.Errorf("%s: %s", ErrRedisInitFailed, err)
				}
				return c, nil
			},
		},
	}

	if err := s.ping(); err != nil {
		return Store{}, err
	}

	return s, nil
}

func (s Store) Set(input types.SetItemInput) error {
	panic("implement me")
}

func (s Store) BatchSet(input types.BatchSetItemInput) error {
	panic("implement me")
}

func (s Store) Get(input types.GetItemInput) (found bool, err error) {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return false, err
	}

	c := s.p.Get()
	defer c.Close()

	val, err := redis.String(c.Do("GET", input.Key))
	if err != nil {
		return false, ErrKeyNotFound
	}

	if err := json.Unmarshal([]byte(val), &input.Value); err != nil {
		return true, err
	}

	return true, nil
}

func (s Store) Delete(input types.DeleteItemInput) error {
	panic("implement me")
}

func (s Store) Close() error {
	panic("implement me")
}
