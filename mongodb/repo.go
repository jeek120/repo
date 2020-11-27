// Copyright (c) 2015 - The Event Horizon authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongodb

import (
	"context"
	"errors"
	"github.com/jeek120/eventbus"
	"github.com/jeek120/repo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// ErrCouldNotDialDB is when the database could not be dialed.
var ErrCouldNotDialDB = errors.New("could not dial database")

// ErrNoDBClient is when no database client is set.
var ErrNoDBClient = errors.New("no database client")

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// ErrModelNotSet is when an model factory is not set on the Repo.
var ErrModelNotSet = errors.New("model not set")

// ErrInvalidQuery is when a query was not returned from the callback to FindCustom.
var ErrInvalidQuery = errors.New("invalid query")

// Repo implements an MongoDB repository for entities.
type Repo struct {
	client    *mongo.Client
	db        string
	factoryFn func() eventbus.Data
}

// NewRepo creates a new Repo.
func NewRepo(uri, db string) (*Repo, error) {
	opts := options.Client().ApplyURI(uri)
	opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	opts.SetReadConcern(readconcern.Majority())
	opts.SetReadPreference(readpref.Primary())
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return nil, ErrCouldNotDialDB
	}

	return NewRepoWithClient(client, db)
}

// NewRepoWithClient creates a new Repo with a client.
func NewRepoWithClient(client *mongo.Client, db string) (*Repo, error) {
	if client == nil {
		return nil, ErrNoDBClient
	}

	r := &Repo{
		client: client,
		db:     db,
	}

	return r, nil
}

// Parent implements the Parent method of the eventhorizon.ReadRepo interface.
func (r *Repo) Parent() repo.ReadRepo {
	return nil
}

// Find implements the Find method of the eventhorizon.ReadRepo interface.
func (r *Repo) Find(data eventbus.Data) (eventbus.Data, error) {
	if r.factoryFn == nil {
		return nil, repo.RepoError{
			Err: ErrModelNotSet,
		}
	}

	c := r.client.Database(r.db).Collection(string(data.DataType()))

	entity := r.factoryFn()
	if err := c.FindOne(context.Background(), data).Decode(entity); err == mongo.ErrNoDocuments {
		return nil, repo.RepoError{
			Err:     repo.ErrEntityNotFound,
			BaseErr: err,
		}
	} else if err != nil {
		return nil, repo.RepoError{
			Err: err,
		}
	}

	return entity, nil
}

// Find implements the Find method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindById(ns string, id eventbus.DataId) (eventbus.Data, error) {
	if r.factoryFn == nil {
		return nil, repo.RepoError{
			Err: ErrModelNotSet,
		}
	}

	c := r.client.Database(r.db).Collection(ns)

	entity := r.factoryFn()
	if err := c.FindOne(context.Background(), bson.M{"_id": string(id)}).Decode(entity); err == mongo.ErrNoDocuments {
		return nil, repo.RepoError{
			Err:     repo.ErrEntityNotFound,
			BaseErr: err,
		}
	} else if err != nil {
		return nil, repo.RepoError{
			Err: err,
		}
	}

	return entity, nil
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindAll(ns string) ([]eventbus.Data, error) {
	if r.factoryFn == nil {
		return nil, repo.RepoError{
			Err: ErrModelNotSet,
		}
	}

	c := r.client.Database(r.db).Collection(ns)
	ctx := context.Background()
	cursor, err := c.Find(ctx, bson.M{})
	if err != nil {
		return nil, repo.RepoError{
			Err: err,
		}
	}

	result := []eventbus.Data{}
	for cursor.Next(ctx) {
		entity := r.factoryFn()
		if err := cursor.Decode(entity); err != nil {
			return nil, repo.RepoError{
				Err: err,
			}
		}
		result = append(result, entity)
	}

	if err := cursor.Close(ctx); err != nil {
		return nil, repo.RepoError{
			Err: err,
		}
	}

	return result, nil
}

// The iterator is not thread safe.
type iter struct {
	cursor    *mongo.Cursor
	data      eventbus.Data
	factoryFn func() eventbus.Data
	decodeErr error
}

func (i *iter) Next(ctx context.Context) bool {
	if !i.cursor.Next(ctx) {
		return false
	}

	item := i.factoryFn()
	i.decodeErr = i.cursor.Decode(item)
	i.data = item
	return true
}

func (i *iter) Value() interface{} {
	return i.data
}

func (i *iter) Close(ctx context.Context) error {
	if err := i.cursor.Close(ctx); err != nil {
		return err
	}
	return i.decodeErr
}

// FindCustomIter returns a mgo cursor you can use to stream results of very large datasets
func (r *Repo) FindCustomIter(tb string, f func(context.Context, *mongo.Collection) (*mongo.Cursor, error)) (repo.Iter, error) {
	if r.factoryFn == nil {
		return nil, repo.RepoError{
			Err: ErrModelNotSet,
		}
	}

	ctx := context.Background()
	c := r.client.Database(r.db).Collection(tb)

	cursor, err := f(ctx, c)
	if err != nil {
		return nil, repo.RepoError{
			BaseErr: err,
			Err:     ErrInvalidQuery,
		}
	}
	if cursor == nil {
		return nil, repo.RepoError{
			Err: ErrInvalidQuery,
		}
	}

	return &iter{
		cursor:    cursor,
		factoryFn: r.factoryFn,
	}, nil
}

// FindCustom uses a callback to specify a custom query for returning models.
// It can also be used to do queries that does not map to the model by executing
// the query in the callback and returning nil to block a second execution of
// the same query in FindCustom. Expect a ErrInvalidQuery if returning a nil
// query from the callback.
func (r *Repo) FindCustom(tb string, f func(context.Context, *mongo.Collection) (*mongo.Cursor, error)) ([]interface{}, error) {
	if r.factoryFn == nil {
		return nil, repo.RepoError{
			Err: ErrModelNotSet,
		}
	}

	ctx := context.Background()
	c := r.client.Database(r.db).Collection(tb)

	cursor, err := f(ctx, c)
	if err != nil {
		return nil, repo.RepoError{
			BaseErr: err,
			Err:     ErrInvalidQuery,
		}
	}
	if cursor == nil {
		return nil, repo.RepoError{
			Err: ErrInvalidQuery,
		}
	}

	result := []interface{}{}
	entity := r.factoryFn()
	for cursor.Next(ctx) {
		if err := cursor.Decode(entity); err != nil {
			return nil, repo.RepoError{
				Err: err,
			}
		}
		result = append(result, entity)
		entity = r.factoryFn()
	}
	if err := cursor.Close(ctx); err != nil {
		return nil, repo.RepoError{
			Err: err,
		}
	}

	return result, nil
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo) Save(data eventbus.Data) error {
	if data.Id() == "" {
		return repo.RepoError{
			Err:     repo.ErrCouldNotSaveEntity,
			BaseErr: repo.ErrMissingEntityID,
		}
	}

	c := r.client.Database(r.db).Collection(string(data.DataType()))

	ctx := context.Background()
	if _, err := c.UpdateOne(ctx,
		bson.M{
			"_id": data.Id(),
		},
		bson.M{
			"$set": data,
		},
		options.Update().SetUpsert(true),
	); err != nil {
		return repo.RepoError{
			Err:     repo.ErrCouldNotSaveEntity,
			BaseErr: err,
		}
	}
	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(data eventbus.Data) error {
	c := r.client.Database(r.db).Collection(string(data.DataType()))

	if r, err := c.DeleteOne(context.Background(), bson.M{"_id": data.Id()}); err != nil {
		return repo.RepoError{
			Err: err,
		}
	} else if r.DeletedCount == 0 {
		return repo.RepoError{
			Err: repo.ErrEntityNotFound,
		}
	}

	return nil
}

// Collection lets the function do custom actions on the collection.
func (r *Repo) Collection(tb string, f func(context.Context, *mongo.Collection) error) error {
	c := r.client.Database(r.db).Collection(tb)

	ctx := context.Background()
	if err := f(ctx, c); err != nil {
		return repo.RepoError{
			Err: err,
		}
	}

	return nil
}

// SetEntityFactory sets a factory function that creates concrete entity types.
func (r *Repo) SetEntityFactory(f func() eventbus.Data) {
	r.factoryFn = f
}

// Clear clears the read model database.
func (r *Repo) Clear(tb string) error {
	c := r.client.Database(r.db).Collection(tb)

	ctx := context.Background()
	if err := c.Drop(ctx); err != nil {
		return repo.RepoError{
			Err:     ErrCouldNotClearDB,
			BaseErr: err,
		}
	}
	return nil
}

// Close closes a database session.
func (r *Repo) Close() {
	r.client.Disconnect(context.Background())
}

// Repository returns a parent ReadRepo if there is one.
func Repository(repo repo.ReadRepo) *Repo {
	if repo == nil {
		return nil
	}

	if r, ok := repo.(*Repo); ok {
		return r
	}

	return Repository(repo.Parent())
}
