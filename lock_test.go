// Copyright 2018, Square, Inc.

package lock_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	//"github.com/mongo-go/testdb"

	"github.com/dmichel1/mongo-lock"
)

func random() string {
	n := 5
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%X", b)
}


var testDb *mongo.Client

func setup(t *testing.T) (string, *mongo.Client) {
	collection := random()

	//if testDb == nil {
		testDb, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
		if err != nil {
			t.Fatal(err)
		}
	//}

	// Add the required unique index on the 'resource' field.
	index := mongo.IndexModel{
		Keys:    bson.M{"resource": 1},
		Options: options.Index().SetUnique(true).SetBackground(false).SetSparse(true),
	}

	_, err = testDb.Database("test").Collection(collection).Indexes().CreateOne(context.Background(), index)
	if err != nil {
		t.Fatal(err)
	}

	return collection, testDb
}

func teardown(t *testing.T, c *mongo.Client, collection string) {
	//if testDb == nil {
	//	t.Errorf("must call setup before teardown")
	//}

	if err := c.Database("test").Collection(collection).Drop(context.Background()); err != nil {
		t.Error(err)
	}
}

/*
func TestCreateIndexes(t *testing.T) {
	coll := setup(t)
	defer teardown(t, coll)

	client := lock.NewClient(coll.Database.Session, coll.Database.Name, coll.Name)
	err := client.CreateIndexes()
	if err != nil {
		t.Error(err)
	}

	indexes, err := coll.Indexes()
	if err != nil {
		t.Error(err)
	}

	expectedIndexes := []mgo.Index{
		{Key: []string{"_id"}, Name: "_id_"},
		{Key: []string{"exclusive.ExpiresAt"}, Name: "exclusive.ExpiresAt_1"},
		{Key: []string{"exclusive.LockId"}, Name: "exclusive.LockId_1"},
		{Key: []string{"resource"}, Name: "resource_1", Unique: true, Sparse: true},
		{Key: []string{"shared.locks.ExpiresAt"}, Name: "shared.locks.ExpiresAt_1"},
		{Key: []string{"shared.locks.LockId"}, Name: "shared.locks.LockId_1"},
	}

	for i, idx := range indexes {
		if diff := deep.Equal(idx, expectedIndexes[i]); diff != nil {
			t.Error(diff)
		}
	}
}
*/


func TestLockExclusive(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	//m, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))




	// Create some locks.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = client.XLock(ctx,"resource2", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = client.XLock(ctx,"resource3", "bbbb", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}

	// Try to lock something that's already locked.
	err = client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
	err = client.XLock(ctx, "resource1", "zzzz", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}

	// Create a lock with some resource name and some lockId
	// that resource expires after some time
	// then with same resource name and lockId can able to create lock.
	err = client.XLock(ctx, "resource4", "cccc", lock.LockDetails{TTL: 1})
	if err != nil {
		t.Error(err)
	}

	// Waiting to expire the lock with resource name "resource4" and lockId "cccc".
	time.Sleep(1100 * time.Millisecond)

	// Try to lock "reource4" with "cccc", which is already expired.
	err = client.XLock(ctx, "resource4", "cccc", lock.LockDetails{})
	if err != nil {
		t.Errorf("err = %s, failed to lock due to the resource already being locked", err)
	}

}

func TestLockShared(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Create some locks.
	err := client.SLock(ctx, "resource1", "aaaa", lock.LockDetails{}, 10)
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx, "resource1", "bbbb", lock.LockDetails{}, 10)
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx, "resource2", "bbbb", lock.LockDetails{}, 10)
	if err != nil {
		t.Error(err)
	}

	// Try to create a shared lock that already exists.
	err = client.SLock(ctx, "resource1", "aaaa", lock.LockDetails{}, 10)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
	err = client.SLock(ctx, "resource2", "bbbb", lock.LockDetails{}, 10)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
}

func TestLockMaxConcurrent(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Create some locks.
	err := client.SLock(ctx, "resource1", "aaaa", lock.LockDetails{}, 2)
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx, "resource1", "bbbb", lock.LockDetails{}, 2)
	if err != nil {
		t.Error(err)
	}

	// Try to create a third lock, which will be more than maxConcurrent.
	err = client.SLock(ctx, "resource1", "cccc", lock.LockDetails{}, 2)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
}

func TestLockInteractions(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Trying to create a shared lock on a resource that already has an
	// exclusive lock in it should return an error.
	err := client.XLock(ctx,"resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource1", "bbbb", lock.LockDetails{}, -1)
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}

	// Trying to create an exclusive lock on a resource that already has a
	// shared lock in it should return an error.
	err = client.SLock(ctx,"resource2", "aaaa", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = client.XLock(ctx,"resource2", "bbbb", lock.LockDetails{})
	if err != lock.ErrAlreadyLocked {
		t.Errorf("err = %s, expected the lock to fail due to the resource already being locked", err)
	}
}

func TestUnlock(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Unlock an exclusive lock.
	err := client.XLock(ctx,"resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	unlocked, err := client.Unlock(ctx,"aaaa")
	if err != nil {
		t.Error(err)
	}
	if len(unlocked) != 1 {
		t.Errorf("%d resources unlocked, expected %d", len(unlocked), 1)
	}
	if unlocked[0].Resource != "resource1" && unlocked[0].LockId != "aaaa" {
		t.Errorf("did not unlock the correct thing")
	}

	// Unlock a shared lock.
	err = client.SLock(ctx,"resource2", "bbbb", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	unlocked, err = client.Unlock(ctx,"bbbb")
	if err != nil {
		t.Error(err)
	}
	if len(unlocked) != 1 {
		t.Errorf("%d resources unlocked, expected %d", len(unlocked), 1)
	}
	if unlocked[0].Resource != "resource2" && unlocked[0].LockId != "bbbb" {
		t.Errorf("did not unlock the correct thing")
	}

	// Try to unlock a lockId that doesn't exist.
	unlocked, err = client.Unlock(ctx,"zzzz")
	if err != nil {
		t.Error(err)
	}
	if len(unlocked) != 0 {
		t.Errorf("%d resources unlocked, expected %d", len(unlocked), 0)
	}
}

func TestUnlockOrder(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Create some locks.
	err := client.XLock(ctx,"resource1", "aaaa", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource4", "aaaa", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = client.XLock(ctx,"resource3", "bbbb", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource2", "bbbb", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource2", "aaaa", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}

	// Make sure they are unlocked in the order of newest to oldest.
	unlocked, err := client.Unlock(ctx,"aaaa")
	if err != nil {
		t.Error(err)
	}

	actual := []string{}
	for _, l := range unlocked {
		actual = append(actual, l.Resource)
	}

	expected := []string{"resource2", "resource4", "resource1"}
	if diff := deep.Equal(actual, expected); diff != nil {
		t.Error(diff)
	}
}

func TestStatusFilterTTLgte(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	_, err := initLockStatusLocks(client)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL greater than.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		TTLgte: 3700,
	}
	actual, err := client.Status(ctx, f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource4",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
		{
			Resource: "resource2",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}
}

func TestStatusFilterTTLlt(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	_, err := initLockStatusLocks(client)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL less than. Shouldn't include locks with no TTL.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		TTLlt: 600,
	}
	actual, err := client.Status(ctx, f)
	if err != nil {
		t.Error(err)
	}

	expected := []lock.LockStatus{}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}
}

func TestStatusFilterCreatedAfter(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	recordedTime, err := initLockStatusLocks(client)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on CreatedAfter.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		CreatedAfter: recordedTime,
	}
	actual, err := client.Status(ctx,f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource4",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
		{
			Resource: "resource3",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "smith",
		},
		{
			Resource: "resource2",
			LockId:   "cccc",
			Type:     lock.LOCK_TYPE_SHARED,
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}
}

func TestStatusFilterCreatedBefore(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	recordedTime, err := initLockStatusLocks(client)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on CreatedBefore.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		CreatedBefore: recordedTime,
	}
	actual, err := client.Status(ctx, f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource2",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "smith",
		},
		{
			Resource: "resource2",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "john",
			Host:     "host.name",
		},
		{
			Resource: "resource1",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "john",
			Host:     "host.name",
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}
}

func TestStatusFilterOwner(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	_, err := initLockStatusLocks(client)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on Owner.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		Owner: "smith",
	}
	actual, err := client.Status(ctx,f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource3",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "smith",
		},
		{
			Resource: "resource2",
			LockId:   "bbbb",
			Type:     lock.LOCK_TYPE_SHARED,
			Owner:    "smith",
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}
}

func TestStatusFilterMultiple(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	_, err := initLockStatusLocks(client)
	if err != nil {
		t.Error(err)
	}

	///////////////////////////////////////////////////////////////////////
	// Filter on TTL, Resource, and LockId.
	///////////////////////////////////////////////////////////////////////
	f := lock.Filter{
		TTLlt:    5000,
		Resource: "resource1",
		LockId:   "aaaa",
	}
	actual, err := client.Status(ctx,f)
	if err != nil {
		t.Error(err)
	}

	// These must be in the order of LockStatusesByCreatedAtDesc.
	expected := []lock.LockStatus{
		{
			Resource: "resource1",
			LockId:   "aaaa",
			Type:     lock.LOCK_TYPE_EXCLUSIVE,
			Owner:    "john",
			Host:     "host.name",
		},
	}

	err = validateLockStatuses(actual, expected)
	if err != nil {
		t.Error(err)
	}
}

func TestStatusTTLValue(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Create a lock with a TTL.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{TTL: 3600})
	if err != nil {
		t.Error(err)
	}
	// Create a lock without a TTL.
	err = client.XLock(ctx,"resource2", "bbbb", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	// Create a lock with a low TTL.
	err = client.XLock(ctx,"resource3", "cccc", lock.LockDetails{TTL: 1})
	if err != nil {
		t.Error(err)
	}

	// Make sure we get back a similar TTL when querying the status of the
	// lock with a TTL.
	f := lock.Filter{
		LockId: "aaaa",
	}
	actual, err := client.Status(ctx,f)
	if err != nil {
		t.Error(err)
	}

	if len(actual) != 1 {
		t.Errorf("got the status of %d locks, expected %d", len(actual), 1)
	}

	if actual[0].TTL > 3600 || actual[0].TTL < 3575 {
		t.Errorf("ttl = %d, expected it to be between 3575 and 3600", actual[0].TTL)
	}

	// Make sure we get back -1 for the TTL for the lock without one.
	f = lock.Filter{
		LockId: "bbbb",
	}
	actual, err = client.Status(ctx,f)
	if err != nil {
		t.Error(err)
	}

	if len(actual) != 1 {
		t.Errorf("got the status of %d locks, expected %d", len(actual), 1)
	}

	if actual[0].TTL != -1 {
		t.Errorf("ttl = %d, expected %d", actual[0].TTL, -1)
	}

	// Sleep for 2 seconds to ensure that the lock on resource3 expired at
	// least 2 seconds ago.
	time.Sleep(time.Duration(2100) * time.Millisecond)

	// Make sure we get back 0 for the TTL of the expired lock.
	f = lock.Filter{
		LockId: "cccc",
	}
	actual, err = client.Status(ctx,f)
	if err != nil {
		t.Error(err)
	}

	if len(actual) != 1 {
		t.Errorf("got the status of %d locks, expected %d", len(actual), 1)
	}

	if actual[0].TTL != 0 {
		t.Errorf("ttl = %d, expected %d", actual[0].TTL, 0)
	}
}

func TestRenew(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Create some locks.
	err := client.XLock(ctx, "resource1", "aaaa", lock.LockDetails{TTL: 3600})
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource4", "aaaa", lock.LockDetails{TTL: 3600}, -1)
	if err != nil {
		t.Error(err)
	}
	err = client.XLock(ctx,"resource3", "bbbb", lock.LockDetails{})
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource2", "bbbb", lock.LockDetails{}, -1)
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource2", "aaaa", lock.LockDetails{TTL: 3600}, -1)
	if err != nil {
		t.Error(err)
	}

	// Verify that locks with the given lockId have their TTL updated.
	renewed, err := client.Renew(ctx,"aaaa", 7200)
	if err != nil {
		t.Error(err)
	}

	if len(renewed) != 3 {
		t.Errorf("%d locks renewed, expected %d", len(renewed), 3)
	}

	f := lock.Filter{
		LockId: "aaaa",
	}
	actual, err := client.Status(ctx,f)
	if err != nil {
		t.Error(err)
	}

	if len(actual) != 3 {
		t.Errorf("got the status of %d locks, expected %d", len(actual), 1)
	}

	for _, a := range actual {
		if a.TTL > 7200 || a.TTL < 7175 {
			t.Errorf("ttl = %d for resource=%s lockId=%s, expected it to be between 7175 and 7200",
				a.TTL, a.Resource, a.LockId)
		}
	}
}

func TestRenewLockIdNotFound(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Create a lock.
	err := client.XLock(ctx,"resource1", "aaaa", lock.LockDetails{TTL: 3600})
	if err != nil {
		t.Error(err)
	}

	renewed, err := client.Renew(ctx,"bbbb", 7200)
	if err != lock.ErrLockNotFound {
		t.Errorf("err = %s, expected the renew to fail due to the lockId not existing", err)
	}

	if len(renewed) != 0 {
		t.Errorf("%d locks renewed, expected %d", len(renewed), 0)
	}
}

func TestRenewTTLExpired(t *testing.T) {
	coll, m := setup(t)
	defer teardown(t, m, coll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	client := lock.NewClient(m, "test", coll)

	// Create some locks.
	err := client.XLock(ctx,"resource1", "aaaa", lock.LockDetails{TTL: 3600})
	if err != nil {
		t.Error(err)
	}
	err = client.SLock(ctx,"resource4", "aaaa", lock.LockDetails{TTL: 1}, -1)
	if err != nil {
		t.Error(err)
	}

	// Sleep for a short time so that we know the TTL of the second lock
	// will be < 1.
	time.Sleep(time.Duration(100) * time.Millisecond)

	// Make sure the renew fails due to the TTL being expired on one of
	// the locks.
	renewed, err := client.Renew(ctx,"aaaa", 7200)
	if err != lock.ErrLockNotFound {
		t.Errorf("err = %s, expected the renew to fail due to the TTL of a lock being < 1", err)
	}

	if len(renewed) > 1 {
		t.Errorf("%d locks renewed, expected a max of %d", len(renewed), 1)
	}
}

// ------------------------------------------------------------------------- //

// initLockStatusLocks initializes locks that are used for the LockStatus tests.
// It returns a time.Time that can be used in tests to filter on CreatedAt.
func initLockStatusLocks(client *lock.Client) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 60)
	defer cancel()
	// Create a bunch of different locks.
	aaaaDetails := lock.LockDetails{
		Owner: "john",
		Host:  "host.name",
		TTL:   3600,
	}
	bbbbDetails := lock.LockDetails{
		Owner: "smith",
	}
	ccccDetails := lock.LockDetails{
		TTL: 7200,
	}
	err := client.XLock(ctx,"resource1", "aaaa", aaaaDetails)
	if err != nil {
		return time.Time{}, err
	}
	err = client.SLock(ctx,"resource2", "aaaa", aaaaDetails, -1)
	if err != nil {
		return time.Time{}, err
	}
	err = client.SLock(ctx,"resource2", "bbbb", bbbbDetails, -1)
	if err != nil {
		return time.Time{}, err
	}

	// Capture a timestamp after the locks that have already been created,
	// and before the additional locks we are about to create. This is used
	// by tests that filter on CreatedAt.
	time.Sleep(time.Duration(1) * time.Millisecond)
	recordedTime := time.Now()
	time.Sleep(time.Duration(1) * time.Millisecond)

	err = client.SLock(ctx,"resource2", "cccc", ccccDetails, -1)
	if err != nil {
		return time.Time{}, err
	}
	err = client.XLock(ctx,"resource3", "bbbb", bbbbDetails)
	if err != nil {
		return time.Time{}, err
	}
	err = client.SLock(ctx,"resource4", "cccc", ccccDetails, -1)
	if err != nil {
		return time.Time{}, err
	}

	return recordedTime, nil
}

// validateLockStatuses compares two slices of LockStatuses, returning an error
// if they are not the same. It zeros out some of the fields on the structs in
// the "actual" argument to make comparisons easier (and still accurate for the
// most part).
func validateLockStatuses(actual, expected []lock.LockStatus) error {
	// Sort actual to make checks deterministic. expected should already
	// be in the LockStatusesByCreatedAtDesc order, but we still need to
	// convert it to the correct type.
	var actualSorted lock.LockStatusesByCreatedAtDesc
	var expectedSorted lock.LockStatusesByCreatedAtDesc
	actualSorted = actual
	expectedSorted = expected
	sort.Sort(actualSorted)

	// Zero out some of the fields in the actual LockStatuses that make
	// it hard to do comparisons and also aren't necessary for this function.
	for i := range actualSorted {
		actualSorted[i].CreatedAt = time.Time{}
		actualSorted[i].RenewedAt = nil
		actualSorted[i].TTL = 0
	}

	if diff := deep.Equal(actualSorted, expectedSorted); diff != nil {
		return fmt.Errorf("%#v", diff)
	}

	return nil
}
