package cache

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/jeek120/eventbus"
	"github.com/jeek120/repo"
)

type namespace eventbus.DataType

// Repo is a middleware that adds caching to a read repository. It will update
// the cache when it receives events affecting the cached items. The primary
// purpose is to use it with smaller collections accessed often.
// Note that there is no limit to the cache size.
type Repo struct {
	repo.ReadWriteRepo
	cache map[namespace]*lru.Cache
}

// NewRepo creates a new Repo.
func NewRepo(repo repo.ReadWriteRepo) *Repo {
	return &Repo{
		ReadWriteRepo: repo,
		cache:         make(map[namespace]*lru.Cache, 0),
	}
}

// Parent implements the Parent method of the eventhorizon.ReadRepo interface.
func (r *Repo) Parent() repo.ReadRepo {
	return r.ReadWriteRepo
}

// Find implements the Find method of the eventhorizon.ReadModel interface.
func (r *Repo) FindById(ns string, id eventbus.DataId) (eventbus.Data, error) {
	entity, ok := r.cache[namespace(ns)].Get(id)
	if ok {
		return entity.(eventbus.Data), nil
	}

	// Fetch and store the entity in the cache.
	entity, err := r.ReadWriteRepo.FindById(ns, id)
	if err != nil {
		return nil, err
	}
	r.cache[namespace(ns)].Add(id, entity)

	return entity.(eventbus.Data), nil
}

// Find implements the Find method of the eventhorizon.ReadModel interface.
func (r *Repo) Find(data eventbus.Data) (eventbus.Data, error) {
	ns := namespace(data.DataType())

	entity, ok := r.cache[ns].Get(data.Id())
	if ok {
		return entity.(eventbus.Data), nil
	}

	// Fetch and store the entity in the cache.
	entity, err := r.ReadWriteRepo.Find(data)
	if err != nil {
		return nil, err
	}
	r.cache[ns].Add(data.Id(), entity)

	return entity.(eventbus.Data), nil
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindAll(ns string) ([]eventbus.Data, error) {
	entities, err := r.ReadWriteRepo.FindAll(ns)
	if err != nil {
		return nil, err
	}

	// Cache all items.
	for _, entity := range entities {
		data := entity.(eventbus.Data)
		r.cache[namespace(data.DataType())].Add(data.Id(), data)
	}

	return entities, nil
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo) Save(data eventbus.Data) error {
	// Bust the cache on save.
	r.cache[namespace(data.DataType())].Remove(data.Id())

	return r.ReadWriteRepo.Save(data)
}

func (r *Repo) Register(ns eventbus.DataType, size int) {
	if _, ok := r.cache[namespace(ns)]; !ok {
		c, err := lru.New(size)
		if err != nil {
			panic(err)
		}
		r.cache[namespace(ns)] = c
	} else {
		panic("cache namespace(" + ns + ") alrealy registed.")
	}
}

func (r *Repo) Merge(data eventbus.Data, merge func(old eventbus.Data)) bool {
	// Bust the cache on save.
	if _old, ok := r.cache[namespace(data.DataType())].Get(data.Id()); ok {
		old := _old.(eventbus.Data)
		merge(old)
		return ok
	} else {
		r.cache[namespace(data.DataType())].Add(data.Id(), data)
		return ok
	}
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(data eventbus.Data) error {
	// Bust the cache on remove.
	r.cache[namespace(data.DataType())].Remove(data.Id())

	return r.ReadWriteRepo.Remove(data)
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
