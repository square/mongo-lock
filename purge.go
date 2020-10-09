// Copyright 2018, Square, Inc.

package lock

import (
	"context"
)

// A Purger deletes expired locks.
type Purger interface {
	// Purge deletes expired locks. It is important to note that not just
	// locks with a TTL of 0 get deleted - any lock that shares a lockId
	// with a lock that has has a TTL of 0 will be removed.
	Purge(ctx context.Context) ([]LockStatus, error)
}

// purge implements the Purger interface.
type purger struct {
	client *Client
}

// NewPurger creates a new Purger.
func NewPurger(client *Client) Purger {
	return &purger{
		client: client,
	}
}

func (p *purger) Purge(ctx context.Context,) ([]LockStatus, error) {
	// Get all locks with a TTL of less than 1, i.e. 0.
	filter := Filter{
		TTLlt: 1,
	}
	locks, err := p.client.Status(ctx, filter)
	if err != nil {
		return []LockStatus{}, err
	}

	// Unlock everything we got.
	allUnlocked := []LockStatus{}
	for _, lock := range locks {
		unlocked, err := p.client.Unlock(ctx, lock.LockId)
		if err != nil {
			return unlocked, err
		}
		allUnlocked = append(allUnlocked, unlocked...)
	}

	return allUnlocked, nil
}
