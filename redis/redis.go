package redis

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/simar7/gokv/encoding"

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
	Codec                encoding.Codec
}

var DefaultOptions = Options{
	MaxIdleConnections:   80,
	MaxActiveConnections: 10000,
	Network:              "tcp",
	Codec:                encoding.JSON,
}

type Store struct {
	p     *redis.Pool
	codec encoding.Codec
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

	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
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
		codec: options.Codec,
	}

	if err := s.ping(); err != nil {
		return Store{}, err
	}

	return s, nil
}

func (s Store) Set(input types.SetItemInput) error {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return err
	}

	c := s.p.Get()
	defer c.Close()

	b, err := s.codec.Marshal(input.Value)
	if err != nil {
		return err
	}

	_, err = redis.String(c.Do("SET", input.Key, string(b)))
	if err != nil {
		return err
	}

	return nil
}

func (s Store) BatchSet(input types.BatchSetItemInput) error {
	c := s.p.Get()
	defer c.Close()

	for i := 0; i < len(input.Keys); i++ {
		if err := util.CheckKeyAndValue(input.Keys[i], input.Values); err != nil {
			return err
		}

		val := reflect.ValueOf(input.Values).Index(i).Interface()
		b, err := s.codec.Marshal(val)
		if err != nil {
			return err
		}

		if err := c.Send("SET", input.Keys[i], string(b)); err != nil {
			return err
		}
	}

	if err := c.Flush(); err != nil {
		return err
	}

	return nil
}

func (s Store) Get(input types.GetItemInput) (found bool, err error) {
	if err := util.CheckKeyAndValue(input.Key, input.Value); err != nil {
		return false, err
	}

	c := s.p.Get()
	defer c.Close()

	val, err := redis.Bytes(c.Do("GET", input.Key))
	if err != nil {
		return false, ErrKeyNotFound
	}

	if err := s.codec.Unmarshal(val, &input.Value); err != nil {
		return true, err
	}

	return true, nil
}

func (s Store) Delete(input types.DeleteItemInput) error {
	if err := util.CheckKey(input.Key); err != nil {
		return err
	}

	c := s.p.Get()
	defer c.Close()

	keysDeleted, err := c.Do("DEL", input.Key)
	if err != nil {
		return err
	}

	if keysDeleted.(int64) <= 0 {
		return ErrKeyNotFound
	}

	return nil
}

func (s Store) Close() error {
	return s.p.Close()
}

func (s Store) Scan(input types.ScanInput) (types.ScanOutput, error) {
	keys, err := s.getAllKeys()
	if err != nil {
		return types.ScanOutput{}, err
	}

	values, err := s.getAllValues(keys)
	if err != nil {
		return types.ScanOutput{}, err
	}

	return types.ScanOutput{
		Keys:   keys,
		Values: values,
	}, nil
}

func (s Store) getAllValues(keys []string) ([][]byte, error) {
	c := s.p.Get()

	var values [][]byte
	var args []interface{}

	for _, k := range keys {
		args = append(args, k)
	}
	vals, err := redis.Values(c.Do("MGET", args...))
	if err != nil {
		return nil, err
	}

	for _, value := range vals {
		values = append(values, value.([]byte))
	}

	return values, nil
}

func (s Store) getAllKeys() ([]string, error) {
	c := s.p.Get()

	iter := 0
	var keys []string
	for {
		if arr, err := redis.Values(c.Do("SCAN", iter)); err != nil {
			return nil, err
		} else {
			iter, _ = redis.Int(arr[0], nil)
			keys, _ = redis.Strings(arr[1], nil)
		}

		if iter == 0 {
			break
		}
	}
	return keys, nil
}
