// Copyright 2018, Square, Inc.

// Package lock provides distributed locking backed by MongoDB.
package lock

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// LOCK_TYPE_EXCLUSIVE is the string representation of an exclusive lock.
	LOCK_TYPE_EXCLUSIVE = "exclusive"

	// LOCK_TYPE_SHARED is the string representation of a shared lock.
	LOCK_TYPE_SHARED = "shared"
)

var (
	// ErrAlreadyLocked is returned by a locking operation when a resource
	// is already locked.
	ErrAlreadyLocked = errors.New("unable to acquire lock (resource is already locked)")

	// ErrLockNotFound is returned when a lock cannot be found.
	ErrLockNotFound = errors.New("unable to find lock")

	UPSERT    = true
	ReturnDoc = options.After
)

// LockDetails contains fields that are used when creating a lock.
type LockDetails struct {
	// The user that is creating the lock.
	Owner string
	// The host that the lock is being created from.
	Host string
	// The time to live (TTL) for the lock, in seconds. Setting this to 0
	// means that the lock will not have a TTL.
	TTL uint
}

// LockStatus represents the status of a lock.
type LockStatus struct {
	// The name of the resource that the lock is on.
	Resource string
	// The id of the lock.
	LockId string
	// The type of the lock ("exclusive" or "shared")
	Type string
	// The name of the user who created the lock.
	Owner string
	// The host that the lock was created from.
	Host string
	// The time that the lock was created at.
	CreatedAt time.Time
	// The time that the lock was renewed at, if applicable.
	RenewedAt *time.Time
	// The TTL for the lock, in seconds. A negative value means that the
	// lock does not have a TTL.
	TTL int64
	// The Mongo ObjectId of the lock. Only used internally for sorting.
	objectId primitive.ObjectID
}

// LockStatusesByCreatedAtDesc is a slice of LockStatus structs, ordered by
// CreatedAt descending.
type LockStatusesByCreatedAtDesc []LockStatus

// Filter contains fields that are used to filter locks when querying their
// status. Fields with zero values are ignored.
type Filter struct {
	CreatedBefore time.Time // Only include locks created before this time.
	CreatedAfter  time.Time // Only include locks created after this time.
	TTLlt         uint      // Only include locks with a TTL less than this value, in seconds.
	TTLgte        uint      // Only include locks with a TTL greater than or equal to this value, in seconds.
	Resource      string    // Only include locks on this resource.
	LockId        string    // Only include locks with this lockId.
	Owner         string    // Only include locks with this owner.
}

// lock represents a lock object stored in Mongo.
type lock struct {
	// Use pointers so we can store null values in the db.
	LockId    *string       `bson:"lockId"`
	Owner     *string       `bson:"owner"`
	Host      *string       `bson:"host"`
	CreatedAt *time.Time    `bson:"createdAt"`
	RenewedAt *time.Time    `bson:"renewedAt"`
	ExpiresAt *time.Time    `bson:"expiresAt"` // How TTLs are stored internally.
	Acquired  bool          `bson:"acquired"`
	ObjectId  primitive.ObjectID `bson:"_id,omitempty"`
}

// sharedLocks represents a slice of shared locks stored in Mongo.
type sharedLocks struct {
	Count uint   `bson:"count"`
	Locks []lock `bson:"locks"`
}

// resource represents a resource object stored in Mongo.
type resource struct {
	Name      string      `bson:"resource"`
	Exclusive lock        `bson:"exclusive"`
	Shared    sharedLocks `bson:"shared"`
}

// A Client is a distributed locking client backed by MongoDB. It requires a
// resource name (the object that gets locked) and a lockId (lock identifier)
// when creating a lock. Multiple locks can be created with the same lockId,
// making it easy to unlock or renew a group of related locks at the same time.
// Another reason for using lockIds is to ensure that only the process that
// creates a lock knows the lockId needed to unlock it - knowing a resource name
// alone is not enough to unlock it.
type Client struct {
	client     *mongo.Client
	db         string
	collection string
}

// NewClient creates a new Client.
func NewClient(session *mongo.Client, db, collection string) *Client {
	return &Client{
		client:     session,
		db:         db,
		collection: collection,
	}
}

// CreateIndexes creates the required and recommended indexes for mongo-lock in
// the client's database. Indexes that already exist are skipped.
func (c *Client) CreateIndexes(ctx context.Context) error {
	lockCollection := c.client.Database(c.db).Collection(c.collection)

	indexes := []mongo.IndexModel{
		// Required.
		{
			Keys:    bson.M{"resource": 1},
			Options: options.Index().SetUnique(true).SetBackground(false).SetSparse(true),
		},

		// Optional.
		{Keys:    bson.M{"exclusive.LockId": 1}},
		{Keys:    bson.M{"exclusive.ExpiresAt": 1}},
		{Keys:    bson.M{"shared.locks.LockId": 1}},
		{Keys:    bson.M{"shared.locks.ExpiresAt": 1}},

	}

	for _, idx := range indexes {
		if _, err := lockCollection.Indexes().CreateOne(ctx, idx); err != nil {
			return err
		}
	}
	return nil
}

// XLock creates an exclusive lock on a resource and associates it with the
// provided lockId. Additional details about the lock can be supplied via
// LockDetails.
func (c *Client) XLock(ctx context.Context, resourceName, lockId string, ld LockDetails) error {
	currentTime := time.Now()
	filter := bson.M{
		"resource": resourceName,
		"$or": []bson.M{
			{"exclusive.acquired": false},
			{"exclusive.expiresAt": bson.M{"$lte": &currentTime}},
		},
		"shared.count": 0,
	}

	r := bson.M{"$set": &resource{
			Name:      resourceName,
			Exclusive: lockFromDetails(lockId, ld),
			Shared: sharedLocks{
				Count: 0,
				Locks: []lock{},
			},
		},
	}

	// One of three things will happen when we run this change (upsert).
	// 1) If the resource exists and has any locks on it (shared or
	//    exclusive), we will get a duplicate key error.
	// 2) If the resource exists but doesn't have any locks on it, we will
	//    update it to obtain an exclusive lock.
	// 3) If the resource doesn't exist yet, it will be inserted which will
	//    give us an exclusive lock on it.
	result := c.client.Database(c.db).Collection(c.collection).FindOneAndUpdate(
		ctx,
		filter,
		r,
		&options.FindOneAndUpdateOptions{Upsert: &UPSERT, ReturnDocument: &ReturnDoc})

	rr := map[string]interface{}{}
	err := result.Decode(rr)
	if err != nil {
		if ce, ok:=err.(mongo.CommandError); ok {
			if ce.Code == 11000 {
				return ErrAlreadyLocked
			}
		}
		return err
	}

	// Acquired lock.
	return nil
}

// SLock creates a shared lock on a resource and associates it with the provided
// lockId. Additional details about the lock can be supplied via LockDetails.
//
// The maxConcurrent argument is used to limit the number of shared locks that
// can exist on a resource concurrently. When SLock is called with maxCurrent = N,
// the lock request will fail unless there are less than N shared locks on the
// resource already. If maxConcurrent is negative then there is no limit to the
// number of shared locks that can exist.
func (c *Client) SLock(ctx context.Context, resourceName, lockId string, ld LockDetails, maxConcurrent int) error {
	filter := bson.M{
		"resource":           resourceName,
		"exclusive.acquired": false,
		"shared.locks.lockId": bson.M{
			"$ne": lockId,
		},
	}
	if maxConcurrent >= 0 {
		filter["shared.count"] = bson.M{
			"$lt": maxConcurrent,
		}
	}

	update := bson.M{
		"$inc": bson.M{
			"shared.count": 1,
		},
		"$push": bson.M{
			"shared.locks": lockFromDetails(lockId, ld),
		},
	}

	// One of three things will happen when we run this change (upsert).
	// 1) If the resource exists and has an exclusive lock on it, or if it
	//    exists and has a shared lock on it with the same lockId, we will
	//    get a duplicate key error.
	// 2) If the resource exists but doesn't have an exclusive lock or a
	//    a shared lock with the same lockId on it, we will update it to
	//    obtain a shared lock so long as there are less than maxConcurrent
	//    shared locks on it already.
	// 3) If the resource doesn't exist yet, it will be inserted which will
	//    give us a shared lock on it.

	result := c.client.Database(c.db).Collection(c.collection).FindOneAndUpdate(
		ctx,
		filter,
		update,
		&options.FindOneAndUpdateOptions{Upsert: &UPSERT, ReturnDocument: &ReturnDoc})

	rr := map[string]interface{}{}
	err := result.Decode(rr)

	if err != nil {
		// TODO if mgo.IsDup(err) {
		return ErrAlreadyLocked

	}

	// Acquired lock.
	return nil
}

// Unlock unlocks all locks with the associated lockId. If there are multiple
// locks with the given lockId, they will be unlocked in the reverse order in
// which they were created in (newest to oldest). For every lock that is
// unlocked, a LockStatus struct (representing the lock before it was unlocked)
// is returned. The order of these is the order in which they were unlocked.
//
// An error will only be returned if there is an issue unlocking a lock; an
// error will not be returned if a lock does not exist. If an error is returned,
// it is safe and recommended to retry this method until until there is no error.
func (c *Client) Unlock(ctx context.Context, lockId string) ([]LockStatus, error) {
	// First find the locks associated with the lockId.
	filter := Filter{
		LockId: lockId,
	}
	locks, err := c.Status(ctx, filter)
	if err != nil {
		return []LockStatus{}, err
	}

	// Sort the lock statuses by most recent CreatedAt date first.
	var sortedLocks LockStatusesByCreatedAtDesc
	sortedLocks = locks
	sort.Sort(sortedLocks)

	unlocked := []LockStatus{}
	for _, lock := range sortedLocks {
		var err error
		switch lock.Type {
		case LOCK_TYPE_EXCLUSIVE:
			err = c.xUnlock(ctx, lock.Resource, lock.LockId)
		case LOCK_TYPE_SHARED:
			err = c.sUnlock(ctx, lock.Resource, lock.LockId)
		}
		if err != nil {
			if err == ErrLockNotFound {
				// It's fine if the lock is gone, that's
				// what we want.
				continue
			}
			return unlocked, err
		}
		unlocked = append(unlocked, lock)
	}

	// Unlocks were successful.
	return unlocked, nil
}

// Status returns the status of locks that match the provided filter. Fields
// with zero values in the Filter struct are ignored.
func (c *Client) Status(ctx context.Context, f Filter) ([]LockStatus, error) {
	// Construct two queries, one for exclusive locks and one for shared
	// locks, which will get combined with the "OR" operator before we send
	// the final query to Mongo.
	xQuery := bson.M{}
	sQuery := bson.M{}

	// Create an object that will be used to filter out unmatched shared
	// locks from the results set we will get back from Mongo.
	filterCond := []bson.M{}

	if !f.CreatedBefore.IsZero() {
		xQuery["exclusive.createdAt"] = bson.M{
			"$lt": f.CreatedBefore,
		}
		sQuery["shared.locks.createdAt"] = bson.M{
			"$lt": f.CreatedBefore,
		}
		filterCond = append(filterCond, bson.M{
			"$lt": []interface{}{"$$lock.createdAt", f.CreatedBefore},
		})

	}
	if !f.CreatedAfter.IsZero() {
		xQuery["exclusive.createdAt"] = bson.M{
			"$gt": f.CreatedAfter,
		}
		sQuery["shared.locks.createdAt"] = bson.M{
			"$gt": f.CreatedAfter,
		}
		filterCond = append(filterCond, bson.M{
			"$gt": []interface{}{"$$lock.createdAt", f.CreatedAfter},
		})
	}

	if f.TTLlt > 0 {
		// Subtract 1 second since TTLs are rounded down to the nearest
		// second. So if TTLlt = 1, we want to return everything with a
		// max TTL of 0 (ttlTime = time.Now()).
		ttlTime := time.Now().Add(time.Duration(f.TTLlt-1) * time.Second)
		xQuery["exclusive.expiresAt"] = bson.M{
			"$lt": ttlTime,
		}
		sQuery["shared.locks.expiresAt"] = bson.M{
			"$lt": ttlTime,
		}
		filterCond = append(filterCond, bson.M{
			"$lt": []interface{}{"$$lock.expiresAt", ttlTime},
		}, bson.M{
			// Exclude locks without a TTL.
			"$gt": []interface{}{"$$lock.expiresAt", nil},
		})
	}
	if f.TTLgte > 0 {
		ttlTime := time.Now().Add(time.Duration(f.TTLgte) * time.Second)
		xQuery["exclusive.expiresAt"] = bson.M{
			"$gte": ttlTime,
		}
		sQuery["shared.locks.expiresAt"] = bson.M{
			"$gte": ttlTime,
		}
		filterCond = append(filterCond, bson.M{
			"$gte": []interface{}{"$$lock.expiresAt", ttlTime},
		})
	}

	if f.LockId != "" {
		xQuery["exclusive.lockId"] = f.LockId
		sQuery["shared.locks.lockId"] = f.LockId
		filterCond = append(filterCond, bson.M{
			"$eq": []interface{}{"$$lock.lockId", f.LockId},
		})
	}

	if f.Owner != "" {
		xQuery["exclusive.owner"] = f.Owner
		sQuery["shared.locks.owner"] = f.Owner
		filterCond = append(filterCond, bson.M{
			"$eq": []interface{}{"$$lock.owner", f.Owner},
		})
	}

	// Construct the part of the query that will be used to match locks that
	// satisfy the provided filters.
	match := bson.M{
		"$or": []bson.M{xQuery, sQuery},
	}
	if f.Resource != "" {
		match["resource"] = f.Resource
	}

	// Construct the part of the query that will remove locks that don't
	// satisfy the provided filters from the results set. This is needed
	// because, by default, Mongo will return an entire document even if
	// only part of it matches the search query. So if a resource has 10
	// shared locks on it and only one of them satisfies the filter, we
	// want to remove all of the other 9 locks from the results we get back.
	project := bson.M{
		"shared.locks": bson.M{
			"$filter": bson.M{
				"input": "$shared.locks",
				"as":    "lock",
				"cond": bson.M{
					"$and": filterCond,
				},
			},
		},
		"resource":  true,
		"exclusive": true,
	}

	// Construct the final query.
	query := []bson.M{
		{
			"$match": match,
		},
		{
			"$project": project,
		},
	}


	resources := []resource{}
	//err := c.client.Database(c.db).Collection(c.collection).Pipe(query).All(&resources)
	cur, err := c.client.Database(c.db).Collection(c.collection).Aggregate(ctx, query)
	if err != nil {
		return []LockStatus{}, err
	}

	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var result resource
		err := cur.Decode(&result)
		if err != nil {
			return []LockStatus{}, err
		}

		resources = append(resources, result)
	}
	if err := cur.Err(); err != nil {
		return []LockStatus{}, err
	}
	//err = result.Decode(&resources)
	//if err != nil {
	//return []LockStatus{}, err
	//}

	// Convert the results set to a []LockStatus.
	statuses := []LockStatus{}
	for _, r := range resources {
		if r.Exclusive.Acquired {
			ls := statusFromLock(r.Exclusive)
			ls.Resource = r.Name
			ls.Type = LOCK_TYPE_EXCLUSIVE

			statuses = append(statuses, ls)
		}

		for _, shared := range r.Shared.Locks {
			ls := statusFromLock(shared)
			ls.Resource = r.Name
			ls.Type = LOCK_TYPE_SHARED

			statuses = append(statuses, ls)
		}
	}

	return statuses, nil
}

// Renew updates the TTL of all locks with the associated lockId. The new TTL
// for the locks will be the value of the argument provided. Only locks with a
// TTL greater than or equal to 1 can be renewed, so Renew will return an error
// if any of the locks with the associated lockId have a TTL of 0. This is to
// prevent a potential race condition beteween renewing locks and purging
// expired ones.
//
// A LockStatus struct is returned for each lock that is renewed. The TTL field
// in each struct represents the new TTL. This information can be used to
// ensure that all of the locks created with the lockId have been renewed. If
// not all locks have been renewed, or if an error is returned, the caller
// should assume that none of the locks are safe to use, and they should unlock
// the lockId.
func (c *Client) Renew(ctx context.Context, lockId string, ttl uint) ([]LockStatus, error) {
	// Retrieve all of the locks with the given lockId.
	filter := Filter{
		LockId: lockId,
	}
	locks, err := c.Status(ctx, filter)
	if err != nil {
		return []LockStatus{}, err
	}

	// Return an error if no locks were found.
	if len(locks) == 0 {
		return []LockStatus{}, ErrLockNotFound
	}

	renewedAt := time.Now()
	newExpiration := renewedAt.Add(time.Duration(ttl) * time.Second)

	// Only renew locks that have a TTL of at least 1 second. We MUST do
	// this to prevent a race condition that could otherwise happen between
	// renewing locks and purging locks that have expired. Since locks can
	// only be purged if their TTL is 0, and locks can only be renewed if
	// their TTL is greater than or equal to 1, we avoid this race.
	minExpiresAt := time.Now().Add(time.Duration(1) * time.Second)

	// Update the expiration date for each lock.
	statuses := []LockStatus{}
	selector := bson.M{}
	change := bson.M{}
	for _, lock := range locks {
		if lock.Type == LOCK_TYPE_EXCLUSIVE {
			selector = bson.M{
				"resource":         lock.Resource,
				"exclusive.lockId": lock.LockId,
				"exclusive.expiresAt": bson.M{
					"$gt": minExpiresAt,
				},
			}

			change = bson.M{
				"$set": bson.M{
					"exclusive.expiresAt": newExpiration,
					"exclusive.renewedAt": renewedAt,
				},
			}
		}

		if lock.Type == LOCK_TYPE_SHARED {
			selector = bson.M{
				"resource":            lock.Resource,
				"shared.locks.lockId": lock.LockId,
				"shared.locks.expiresAt": bson.M{
					"$gt": minExpiresAt,
				},
			}

			change = bson.M{
				"$set": bson.M{
					"shared.locks.$.expiresAt": newExpiration,
					"shared.locks.$.renewedAt": renewedAt,
				},
			}
		}

		// One of two things will happen when we run this change.
		// 1) If the lock doesn't exist, or if it does exist but its
		//    TTL is not greater than 1 second, ErrLockNotFound will be
		//    returned.
		// 2) If the lock exists and its TTL is greater than 1 second,
		//    its TTL will be updated.
		result := c.client.Database(c.db).Collection(c.collection).FindOneAndUpdate(
			ctx,
			selector,
			change,
			&options.FindOneAndUpdateOptions{Upsert: &UPSERT, ReturnDocument: &ReturnDoc})

		rr := map[string]interface{}{}
		err := result.Decode(rr)
		if err != nil {
			if _,ok:=err.(mongo.CommandError); ok {
					// TODO not found
						return statuses, ErrLockNotFound
			}
			return statuses, err
		}

		lock.TTL = calcTTL(&newExpiration)
		lock.RenewedAt = &renewedAt
		statuses = append(statuses, lock)
	}

	return statuses, nil
}

// ------------------------------------------------------------------------- //

// xUnlock unlocks an exclusive lock on a resource.
func (c *Client) xUnlock(ctx context.Context, resourceName, lockId string) error {
	selector := bson.M{
		"resource":         resourceName,
		"exclusive.lockId": lockId,
	}

	change := bson.M{
		"$set": bson.M{
			"exclusive": &lock{},
		},
	}

	// One of two things will happen when we run this change.
	// 1) If the resource doesn't exist, or it exists but doesn't have an
	//    exclusive lock with the given lockId on it, ErrLockNotFound will
	//    be returned.
	// 2) If the resource exists and has an exclusive lock with the given
	//    lockId on it, the exclusive lock will be removed from the resource.

	result := c.client.Database(c.db).Collection(c.collection).FindOneAndUpdate(
		ctx,
		selector,
		change,
		&options.FindOneAndUpdateOptions{Upsert: &UPSERT, ReturnDocument: &ReturnDoc})

	rr := map[string]interface{}{}
	err := result.Decode(rr)
	if err != nil {
		if we, ok:=err.(mongo.WriteException); ok {
			for _,e:=range we.WriteErrors {
				if e.Code == 11000 {
					return ErrAlreadyLocked
				}
			}
		}
		return err
	}


	if _, ok:=err.(mongo.CommandError); ok {
		return ErrLockNotFound
	}

	// Unlocked lock.
	return nil
}

// sUnlock unlocks a shared lock on a resource.
func (c *Client) sUnlock(ctx context.Context, resourceName, lockId string) error {
	selector := bson.M{
		"resource":            resourceName,
		"shared.locks.lockId": lockId,
	}

	change := bson.M{
		"$inc": bson.M{
			"shared.count": -1,
		},
		"$pull": bson.M{
			"shared.locks": bson.M{
				"lockId": lockId,
			},
		},
	}

	// One of two things will happen when we run this change.
	// 1) If the resource doesn't exist, or it exists but doesn't have a
	//    shared lock with the given lockId on it, ErrLockNotFound will be
	//    returned.
	// 2) If the resource exists and has a shared lock with the given lockId
	//    on it, the shared lock corresponding to the lockId will be removed
	//    from the resource.


	result := c.client.Database(c.db).Collection(c.collection).FindOneAndUpdate(
		ctx,
		selector,
		change,
		&options.FindOneAndUpdateOptions{Upsert: &UPSERT, ReturnDocument: &ReturnDoc})

	rr := map[string]interface{}{}
	err := result.Decode(rr)
	if err != nil {
		if we, ok:=err.(mongo.WriteException); ok {
			for _,e:=range we.WriteErrors {
				if e.Code == 11000 {
					return ErrAlreadyLocked
				}
			}
		}
		return err
	}

	if _, ok:=err.(mongo.CommandError); ok {
		return ErrLockNotFound
	}

	//if err != nil {
	//	if err == mgo.ErrNotFound {
	//		return ErrLockNotFound
	//	}
	//	return err
	//}

	// Unlocked lock.
	return nil
}

// lockFromDetails creates a lock struct from a lockId and a LockDetails struct.
func lockFromDetails(lockId string, ld LockDetails) lock {
	now := time.Now()

	lock := lock{
		LockId:    &lockId,
		CreatedAt: &now,
		Acquired:  true,
		ObjectId:  primitive.NewObjectID(),
	}

	if ld.Owner != "" {
		lock.Owner = &ld.Owner
	}
	if ld.Host != "" {
		lock.Host = &ld.Host
	}
	if ld.TTL > 0 {
		e := now.Add(time.Duration(ld.TTL) * time.Second)
		lock.ExpiresAt = &e
	}

	return lock
}

// statusFromLock creates a LockStatus struct from a lock struct.
func statusFromLock(l lock) LockStatus {
	ls := LockStatus{
		RenewedAt: l.RenewedAt,
		objectId:  l.ObjectId,
		TTL:       calcTTL(l.ExpiresAt),
	}

	// Be careful and make sure we don't dereference a nil pointer.
	if l.LockId != nil {
		ls.LockId = *l.LockId
	}
	if l.Owner != nil {
		ls.Owner = *l.Owner
	}
	if l.Host != nil {
		ls.Host = *l.Host
	}
	if l.CreatedAt != nil {
		ls.CreatedAt = *l.CreatedAt
	}

	return ls
}

// calcTTL calculates a TTL value, in seconds, by subtracting the current time
// from an expiration time. If the expiration time has already past, it returns
// 0. If there is no expiration time, it returns -1.
func calcTTL(expiresAt *time.Time) int64 {
	if expiresAt == nil {
		// There is no TTL.
		return -1
	}

	delta := expiresAt.Sub(time.Now())
	ttl := int64(delta.Seconds()) // Don't need sub-second granularity.
	if ttl < 0 {
		return 0
	}
	return ttl
}

func (ls LockStatusesByCreatedAtDesc) Len() int { return len(ls) }
func (ls LockStatusesByCreatedAtDesc) Less(i, j int) bool {
	// Sort by Mongo's ObjectId instead of the CreatedAt field because
	// ObjectIds are a true indicator of which lock was created first.
	// CreatedAt works fine most of the time, but it is possible for two
	// locks to have the same CreatedAt value.
	return ls[i].objectId.Timestamp().Nanosecond() > ls[j].objectId.Timestamp().Nanosecond()
}
func (ls LockStatusesByCreatedAtDesc) Swap(i, j int) { ls[i], ls[j] = ls[j], ls[i] }
