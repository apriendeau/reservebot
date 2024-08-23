package data

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/ameliagapin/reservebot/err"
	"github.com/ameliagapin/reservebot/models"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

const (
	reservationsKey string = "reservebot-reservations"
	resourcesKey    string = "reservebot-resources"
)

type RedisReservations struct {
	Reservations []*models.Reservation `json:"reservations"`
}

type RedisResources struct {
	Resources map[string]*models.Resource `json:"resources"`
}

type Redis struct {
	rdb  *redis.Client
	lock sync.Mutex
}

func NewRedis(addr, pass string, db int) *Redis {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pass, // no password set
		DB:       db,   // use default DB
	})

	r := &Redis{
		rdb: rdb,
	}

	return r
}

func (m *Redis) Create(name, env string) error {
	// GetResource creates the resource if it doesn't exist
	r := m.GetResource(name, env, true)
	r.LastActivity = time.Now()

	return nil
}

func (m *Redis) Reserve(u *models.User, name, env string) error {
	r := m.GetResource(name, env, true)

	m.lock.Lock()
	defer m.lock.Unlock()
	reservations := m.GetRedisReservations()
	// check for existing reservation
	for _, res := range reservations {
		if res.User.ID == u.ID {
			if res.Resource.Key() == r.Key() {
				return err.AlreadyInQueue
			}
		}
	}

	res := &models.Reservation{
		User:     u,
		Resource: r,
		Time:     time.Now(),
	}

	reservations = append(reservations, res)
	r.LastActivity = time.Now()

	m.SetRedisReservations(reservations)
	return nil
}
func (m *Redis) GetRedisReservations() []*models.Reservation {
	res := &RedisReservations{}

	str, err := m.rdb.Get(context.Background(), reservationsKey).Result()
	if err != nil {
		m.SetRedisReservations([]*models.Reservation{})

		str, err = m.rdb.Get(context.Background(), reservationsKey).Result()
		if err != nil {
			panic(err)
		}
	}
	if err := json.Unmarshal([]byte(str), res); err != nil {
		panic(err)
	}
	return res.Reservations
}

func (m *Redis) SetRedisReservations(res []*models.Reservation) []*models.Reservation {
	reservations := &RedisReservations{
		Reservations: res,
	}

	b, err := json.Marshal(reservations)
	if err != nil {
		panic(err)
	}

	if err := m.rdb.Set(context.Background(), reservationsKey, string(b), 0).Err(); err != nil {
		panic(err)
	}
	if reservations.Reservations == nil {
		reservations.Reservations = make([]*models.Reservation, 0)
	}
	return reservations.Reservations
}

func (m *Redis) GetRedisResources() map[string]*models.Resource {
	res := &RedisResources{}
	str, err := m.rdb.Get(context.Background(), resourcesKey).Result()
	if err != nil {
		m.SetRedisResources(map[string]*models.Resource{})

		str, err = m.rdb.Get(context.Background(), resourcesKey).Result()
		if err != nil {
			panic(err)
		}
	}
	if err := json.Unmarshal([]byte(str), res); err != nil {
		panic(err)
	}
	if res.Resources == nil {
		res.Resources = make(map[string]*models.Resource, 0)
	}
	return res.Resources
}

func (m *Redis) SetRedisResources(res map[string]*models.Resource) map[string]*models.Resource {
	resources := &RedisResources{
		Resources: res,
	}

	b, err := json.Marshal(resources)
	if err != nil {
		panic(err)
	}

	if err := m.rdb.Set(context.Background(), resourcesKey, string(b), 0).Err(); err != nil {
		panic(err)
	}

	return resources.Resources
}

func (m *Redis) GetReservation(u *models.User, name, env string) *models.Reservation {
	r := m.GetResource(name, env, false)
	if r == nil {
		return nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	reservations := m.GetRedisReservations()
	for _, res := range reservations {
		if res.User.ID == u.ID {
			if res.Resource.Key() == r.Key() {
				return res
			}
		}
	}
	return nil
}

// Remove removes a user from a resource's queue.
// If the removal advances the queue, the new resource holder's reservation will have the time updated
func (m *Redis) Remove(u *models.User, name, env string) error {
	// minor optimization: if the resource doesn't exist, there's no need to loop through all reservations
	r := m.GetResource(name, env, false)
	if r == nil {
		return err.ResourceDoesNotExist
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	reservations := m.GetRedisReservations()

	idx := -1
	pos := 0
	for i, res := range reservations {
		if res.Resource.Key() == r.Key() {
			pos++
			if res.User.ID == u.ID {
				idx = i
				break
			}
		}
	}
	if idx == -1 {
		return err.NotInQueue
	}

	reservations = append(reservations[:idx], reservations[idx+1:]...)

	// if the user was in pos=1, then removal would move new user into pos=1. This should update the time on their res
	if pos == 1 {
		for _, res := range reservations {
			if res.Resource.Key() == r.Key() {
				res.Time = time.Now()
				break
			}
		}
	}

	r.LastActivity = time.Now()
	m.SetRedisReservations(reservations)

	return nil
}

func (m *Redis) GetPosition(u *models.User, name, env string) (int, error) {
	r := m.GetResource(name, env, false)
	if r == nil {
		return 0, err.ResourceDoesNotExist
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	pos := 0
	inQueue := false
	reservations := m.GetRedisReservations()
	for _, res := range reservations {
		if res.Resource.Key() == r.Key() {
			// increment pos first because want to return zero-based index
			pos++
			if res.User.ID == u.ID {
				inQueue = true
				break
			}
		}
	}
	if !inQueue {
		return 0, err.NotInQueue
	}

	return pos, nil
}

func (m *Redis) GetResource(name, env string, create bool) *models.Resource {
	m.lock.Lock()
	defer m.lock.Unlock()

	resources := m.GetRedisResources()
	key := models.ResourceKey(name, env)
	r, ok := resources[key]
	if !ok {
		if create {
			r = &models.Resource{
				Name: name,
				Env:  env,
			}
			resources[r.Key()] = r
			m.SetRedisResources(resources)
		}
	}
	return r
}

func (m *Redis) RemoveResource(name, env string) error {
	r := m.GetResource(name, env, false)
	if r == nil {
		return err.ResourceDoesNotExist
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	reservations := m.GetRedisReservations()
	resources := m.GetRedisResources()

	for idx, res := range reservations {
		if res.Resource.Key() == r.Key() {
			reservations = append(reservations[:idx], reservations[idx+1:]...)
		}
	}
	m.SetRedisReservations(reservations)
	delete(resources, r.Key())
	m.SetRedisResources(resources)

	return nil
}

func (m *Redis) RemoveEnv(name, env string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	reservations := m.GetRedisReservations()
	resources := m.GetRedisResources()

	exists := false
	for idx, res := range reservations {
		if res.Resource.Env == env {
			reservations = append(reservations[:idx], reservations[idx+1:]...)
			exists = true
		}
	}

	for k, res := range resources {
		if res.Env == env {
			delete(resources, k)
			exists = true
		}
	}

	if !exists {
		return err.EnvDoesNotExist
	}

	m.SetRedisReservations(reservations)
	m.SetRedisResources(resources)
	return nil
}

func (m *Redis) GetResources() []*models.Resource {
	m.lock.Lock()
	defer m.lock.Unlock()

	resources := m.GetRedisResources()

	keys := []string{}
	for k, _ := range resources {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ret := []*models.Resource{}
	for _, k := range keys {
		ret = append(ret, resources[k])
	}
	m.SetRedisResources(resources)

	return ret
}

// Does not implement lock
func (m *Redis) GetQueues() []*models.Queue {
	ret := []*models.Queue{}

	resources := m.GetResources()
	for _, r := range resources {
		q, _ := m.GetQueueForResource(r.Name, r.Env)
		ret = append(ret, q)
	}

	return ret
}

func (m *Redis) GetQueueForResource(name, env string) (*models.Queue, error) {
	// minor optimization
	r := m.GetResource(name, env, false)
	if r == nil {
		return nil, err.ResourceDoesNotExist
	}

	ret := &models.Queue{
		Resource: r,
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	reservations := m.GetRedisReservations()
	for _, res := range reservations {
		if res.Resource.Key() == r.Key() {
			ret.Reservations = append(ret.Reservations, res)
		}
	}

	return ret, nil
}

func (m *Redis) GetReservationForResource(name, env string) (*models.Reservation, error) {
	// minor optimization
	r := m.GetResource(name, env, false)
	if r == nil {
		return nil, err.ResourceDoesNotExist
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	reservations := m.GetRedisReservations()
	for _, res := range reservations {
		if res.Resource.Key() == r.Key() {
			return res, nil
		}
	}

	return nil, nil
}

// Does not implement lock
func (m *Redis) GetQueuesForEnv(env string) map[string]*models.Queue {
	ret := make(map[string]*models.Queue)

	resources := m.GetResourcesForEnv(env)
	for _, r := range resources {
		q, _ := m.GetQueueForResource(r.Name, r.Env)
		ret[r.Name] = q
	}

	return ret
}

func (m *Redis) GetResourcesForEnv(env string) []*models.Resource {
	m.lock.Lock()
	defer m.lock.Unlock()

	resources := m.GetRedisResources()

	keys := []string{}
	for k, r := range resources {
		if r.Env == env {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	ret := []*models.Resource{}
	for _, k := range keys {
		ret = append(ret, resources[k])
	}
	return ret
}

func (m *Redis) GetAllUsersInQueues() []*models.User {
	m.lock.Lock()
	defer m.lock.Unlock()

	all := map[string]*models.User{}

	reservations := m.GetRedisReservations()
	for _, r := range reservations {
		all[r.User.ID] = r.User
	}

	ret := []*models.User{}
	for _, u := range all {
		ret = append(ret, u)
	}

	return ret
}

func (m *Redis) ClearQueueForResource(name, env string) error {
	// minor optimization
	r := m.GetResource(name, env, false)
	if r == nil {
		return err.ResourceDoesNotExist
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	reservations := m.GetRedisReservations()
	filtered := []*models.Reservation{}
	for _, res := range reservations {
		if res.Resource.Key() != r.Key() {
			filtered = append(filtered, res)
		}
	}
	reservations = filtered
	r.LastActivity = time.Now()
	m.SetRedisReservations(reservations)

	return nil
}

func (m *Redis) PruneInactiveResources(hours int) error {
	resources := m.GetResources()
	oldestTime := time.Now().Add(-time.Duration(hours) * time.Hour)

	for _, r := range resources {
		q, err := m.GetQueueForResource(r.Name, r.Env)
		if err != nil {
			log.Errorf("%+v", err)
		}
		if q.HasReservations() {
			continue
		}
		if r.LastActivity.Before(oldestTime) {
			err := m.RemoveResource(r.Name, r.Env)
			if err != nil {
				log.Errorf("%+v", err)
			}
		}
	}
	return nil
}
