// Copyright 2019 CanonicalLtd

package simulation

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/juju/utils"
	"github.com/juju/zaputil"
	"github.com/juju/zaputil/zapctx"

	"github.com/cloud-green/sisyphus/config"
	"github.com/cloud-green/sisyphus/simulation/call"
)

var (
	contextDoneError = errors.New("done")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// CallBackend defines the interface used by the simulation to perform
// state transition calls.
type CallBackend interface {
	Do(context.Context, config.Call, call.Attributes) (call.Attributes, error)
}

// New returns a new simulation based on the provided configuration.
func New(config config.Config, callBackend CallBackend, numberOfWorkers int) *Simulation {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Simulation{
		Config:      config,
		Attributes:  config.Constants,
		CallBackend: callBackend,
		ctx:         ctx,
		stop:        cancel,
		e:           newExecutor(numberOfWorkers),
	}

	// start creating root entity sets
	for _, entitySet := range config.RootEntities {
		newEntitySet(ctx, entitySet, copyAttributes(s.Attributes), s)
	}
	return s
}

// Simulation represents a simulation
type Simulation struct {
	config.Config
	CallBackend
	call.Attributes

	ctx    context.Context
	errors chan error
	stop   func()
	e      *executor
}

// Close stops the simulation and releases all used resources.
func (s *Simulation) Close() {
	s.stop()
	if s.e != nil {
		s.e.close()
	}
}

// error means an error occured during execution of one go routine
// and the undelying context should be cancelled.
func (s *Simulation) error(err error) {
	if errors.Cause(err) != contextDoneError {
		zapctx.Error(s.ctx, "an error occured", zaputil.Error(err))
	}
	s.stop()
}

func newEntitySet(ctx context.Context, config config.EntitySet, attributes call.Attributes, sim *Simulation) {
	es := &entitySet{
		EntitySet:  config,
		attributes: attributes,
	}
	sim.e.addJob(ctx, 0, func() {
		es.create(ctx, sim)
	})
}

type entitySet struct {
	config.EntitySet
	attributes call.Attributes

	cfg                     config.Entity
	numberOfEntities        int
	mu                      sync.Mutex
	numberOfCreatedEntities int
}

func (e *entitySet) create(ctx context.Context, sim *Simulation) {
	// check that the configuration for the named entity exists
	cfg, ok := sim.Entities[e.Entity]
	if !ok {
		sim.error(errors.NotFoundf("entity %q", e.Entity))
		return
	}
	e.cfg = cfg

	// determine how many entities are to be created
	c := cardinality(e.Cardinality)
	numberOfEntities, err := c.Value(e.attributes)
	if err != nil {
		sim.error(errors.Trace(err))
		return
	}
	e.numberOfEntities = numberOfEntities
	e.numberOfCreatedEntities = 0

	// add a job that will add all the entities
	sim.e.addJob(ctx, newDelay(e.Timer), func() {
		e.createEntity(ctx, sim)
	})

	return
}

func (e *entitySet) createEntity(ctx context.Context, sim *Simulation) {
	e.mu.Lock()
	if e.numberOfCreatedEntities >= e.numberOfEntities {
		e.mu.Unlock()
		return
	}
	e.numberOfCreatedEntities++
	e.mu.Unlock()

	createEntity(ctx, e.cfg, copyAttributes(e.attributes), sim)

	sim.e.addJob(ctx, newDelay(e.Timer), func() {
		e.createEntity(ctx, sim)
	})
}

func createEntity(ctx context.Context, config config.Entity, attributes call.Attributes, sim *Simulation) {
	// the we sample the entities attributes
	for key, config := range config.Attributes {
		distribution := &AttributeDistribution{
			Attribute: config,
		}
		value, err := distribution.Sample()
		if err != nil {
			sim.error(errors.Trace(err))
			return
		}
		// and add sampled values to the set of attributes (as strings)
		attributes[key] = value
	}

	// if there are any subordinate entities, we create them
	for _, esConfig := range config.Subordinates {
		newEntitySet(ctx, esConfig, attributes, sim)
	}

	// if an initial state is defined, we create it and run the state simulation
	if config.InitialState != "" {
		stateConfig, ok := sim.States[config.InitialState]
		if !ok {
			sim.error(errors.NotFoundf("state %q", config.InitialState))
			return
		}
		s := &State{
			State:      stateConfig,
			Attributes: copyAttributes(attributes),
		}
		sim.e.addJob(ctx, 0, func() {
			s.run(ctx, sim)
		})
	}

}

type State struct {
	config.State
	call.Attributes
}

func (s *State) generateAttributes(ctx context.Context, sim *Simulation) error {
	// the we sample the state attributes if any are defined
	for key, config := range s.State.Attributes {
		distribution := &AttributeDistribution{
			Attribute: config,
		}
		value, err := distribution.Sample()
		if err != nil {
			return errors.Trace(err)
		}
		// and add sampled values to the set of attributes (as strings)
		s.Attributes[key] = value
	}
	return nil
}

func (s *State) run(ctx context.Context, sim *Simulation) {
	// if there are no specified transtions, we just
	// return and end the simulation
	if len(s.Transitions) == 0 {
		return
	}
	delay := newDelay(s.Timer)
	sim.e.addJob(ctx, delay, func() {
		s.runTransition(ctx, sim)
	})
}

func (s *State) runTransition(ctx context.Context, sim *Simulation) {
	err := s.generateAttributes(ctx, sim)
	if err != nil {
		sim.error(errors.Trace(err))
		return
	}

	// calculate the sum of transition weigths
	sum := 0.0
	for _, transition := range s.Transitions {
		if transition.Probability < 0 {
			sim.error(errors.Errorf("negative transition probability %v", transition.Probability))
			return
		}
		sum += transition.Probability
	}
	if sum == 0 {
		sim.error(errors.Errorf("sum of transition probabilities is 0"))
		return
	}
	// create a random number [0 .. sum]
	randomNumber := sum * rand.Float64()
	for _, transition := range s.Transitions {
		// subtract the transition weigth
		randomNumber -= transition.Probability
		// if we reached 0 (or less) we choose this transition
		if randomNumber <= 0 {
			nextStateName := transition.State
			attributes := s.Attributes

			if !isEmptyCall(transition.Call) {
				attributes, err = sim.Do(ctx, transition.Call, s.Attributes)
				if err != nil {
					zapctx.Error(ctx, "error performing call", zaputil.Error(err))
					attributes["error"] = errors.Details(err)
					if transition.OnFailure != "" {
						nextStateName = transition.OnFailure
					}
				}
			}
			nextStateConfig, ok := sim.States[nextStateName]
			if !ok {
				sim.error(errors.NotFoundf("state %q", nextStateName))
				return
			}

			nextState := &State{
				State:      nextStateConfig,
				Attributes: copyAttributes(attributes),
			}
			sim.e.addJob(ctx, 0, func() {
				nextState.run(ctx, sim)
			})
		}
	}
}

func isEmptyCall(call config.Call) bool {
	return call.Method == "" && call.URL == "" && len(call.Parameters) == 0 && len(call.Results) == 0
}

type AttributeDistribution struct {
	config.Attribute
}

// Sample return a sample float64 from the attribute distribution
func (a *AttributeDistribution) Sample() (interface{}, error) {
	switch a.Type {
	case config.ConstantIntAttributeType:
		return a.Value, nil
	case config.MapAttributeType:
		return a.MapValue, nil
	case config.MapListAttributeType:
		return a.MapValues, nil
	case config.RandomIntAttributeType:
		return int(math.Floor(a.Min + (a.Max-a.Min)*rand.Float64())), nil
	case config.PowerIntAttributeType:
		v := rand.Float64()
		nn := a.N + 1
		sample := math.Pow((math.Pow(a.Max, nn)-math.Pow(a.Min, nn))*v+math.Pow(a.Min, nn), (1 / nn))
		return int(math.Floor(sample)), nil
	case config.NormalIntAttributeType:
		return int(math.Floor(math.Abs(rand.NormFloat64()*a.StdDev + a.N))), nil
	case config.RandomFloatAttributeType:
		return a.Min + (a.Max-a.Min)*rand.Float64(), nil
	case config.PowerFloatAttributeType:
		v := rand.Float64()
		nn := a.N + 1
		return math.Pow((math.Pow(a.Max, nn)-math.Pow(a.Min, nn))*v+math.Pow(a.Min, nn), (1 / nn)), nil
	case config.NormalFloatAttributeType:
		return math.Abs(rand.NormFloat64()*a.StdDev + a.N), nil
	case config.ConstantStringAttributeType:
		return a.StringValue, nil
	case config.RandomStringAttributeType:
		uuid, err := utils.NewUUID()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if a.StringValue == "" {
			return uuid.String(), nil
		}
		if a.Min != 0 || a.Max != 0 {
			return fmt.Sprintf("%s%d", a.StringValue, int(math.Floor(a.Min+(a.Max-a.Min)*rand.Float64()))), nil
		}
		return fmt.Sprintf("%s%v", a.StringValue, uuid), nil
	case config.RandomValueAttributeType:
		if len(a.Values) == 0 {
			return nil, errors.New("empty list of values")
		}
		return a.Values[rand.Intn(len(a.Values))], nil
	case config.RandomSubsetAttributeType:
		if len(a.Values) == 0 {
			return nil, errors.New("empty list of values")
		}
		values := make(map[int]bool)
		for i := 0; i < rand.Intn(len(a.Values)); i++ {
			values[rand.Intn(len(a.Values))] = true
		}
		subset := []interface{}{}
		for k, _ := range values {
			subset = append(subset, a.Values[k])
		}
		return subset, nil
	}

	return 0, nil
}

func newDelay(c config.Timer) time.Duration {
	var duration time.Duration
	switch c.Type {
	case config.FixedTimer:
		duration = c.Interval
	case config.RandomTimer:
		duration = time.Duration(int64(c.Min) + rand.Int63n(int64(c.Max-c.Min)))
	case "":
		duration = 0
	}
	return duration
}

type cardinality string

// Value returns the value of the cardinality. It may be a constant integer
// or name of an attribute.
func (c *cardinality) Value(attributes call.Attributes) (int, error) {
	value, ok := attributes[string(*c)]
	if !ok {
		if *c == "" {
			return 1, nil
		}
		return strconv.Atoi(string(*c))
	}
	switch value.(type) {
	case int:
		return value.(int), nil
	case string:
		return strconv.Atoi(value.(string))
	default:
		return -1, errors.Errorf("unknown type: expected int or string, got %T", value)
	}
}

func copyAttributes(a call.Attributes) call.Attributes {
	attributes := call.Attributes(make(map[string]interface{}))
	for k, v := range a {
		attributes[k] = v
	}
	return attributes
}
