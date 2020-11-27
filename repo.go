package repo

import (
	"context"
	"errors"
	"github.com/jeek120/eventbus"
)

// RepoError is an error in the read repository, with the namespace.
type RepoError struct {
	// Err is the error.
	Err error
	// BaseErr is an optional underlying error, for example from the DB driver.
	BaseErr error
}

// Error implements the Error method of the errors.Error interface.
func (e RepoError) Error() string {
	errStr := e.Err.Error()
	if e.BaseErr != nil {
		errStr += ": " + e.BaseErr.Error()
	}
	return errStr
}

// ErrEntityNotFound is when a entity could not be found.
var ErrEntityNotFound = errors.New("could not find entity")

// ErrCouldNotSaveEntity is when a entity could not be saved.
var ErrCouldNotSaveEntity = errors.New("could not save entity")

// ErrMissingEntityID is when a entity has no ID.
var ErrMissingEntityID = errors.New("missing entity ID")

// ReadRepo is a read repository for entities.
type ReadRepo interface {
	// Parent returns the parent read repository, if there is one.
	// Useful for iterating a wrapped set of repositories to get a specific one.
	Parent() ReadRepo

	// Find returns an entity for an ID.
	Find(data eventbus.Data) (eventbus.Data, error)
	FindById(ns string, id eventbus.DataId) (eventbus.Data, error)

	// FindAll returns all entities in the repository.
	FindAll(ns string) ([]eventbus.Data, error)
}

// WriteRepo is a write repository for entities.
type WriteRepo interface {
	// Save saves a entity in the storage.
	Save(data eventbus.Data) error

	// Remove removes a entity by ID from the storage.
	Remove(data eventbus.Data) error
}

// ReadWriteRepo is a combined read and write repo, mainly useful for testing.
type ReadWriteRepo interface {
	ReadRepo
	WriteRepo
}

// ErrEntityHasNoVersion is when an entity has no version number.
var ErrEntityHasNoVersion = errors.New("entity has no version")

// ErrIncorrectEntityVersion is when an entity has an incorrect version.
var ErrIncorrectEntityVersion = errors.New("incorrect entity version")

// Iter is a stateful iterator object that when called Next() readies the next
// value that can be retrieved from Value(). Enables incremental object retrieval
// from repos that support it. You must call Close() on each Iter even when
// results were delivered without apparent error.
type Iter interface {
	Next(ctx context.Context) bool
	Value() interface{}
	Close(ctx context.Context) error
}
