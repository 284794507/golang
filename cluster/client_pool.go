package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool"
	"go-redis/lib/logger"
	client2 "go-redis/resp/client"
)

type connnectionFactory struct {
	Peer string
}

func (c connnectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	client, err := client2.MakeClient(c.Peer)
	if err != nil {
		return nil, err
	}
	client.Start()
	return pool.NewPooledObject(client), err
}

func (c connnectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	client, ok := object.Object.(*client2.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	client.Close()
	return nil
}

func (c connnectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	logger.Debug("ValidateObject")
	return true
}

func (c connnectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	logger.Debug("ActivateObject")
	return nil
}

func (c connnectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	logger.Debug("PassivateObject")
	return nil
}
