// Copyright 2019 CanonicalLtd

package config

import (
	"time"
)

// Config holds configuration data for the user simulation.
type Config struct {
	// Constants contains simulation wide constants
	Constants map[string]interface{} `yaml:"constants"`
	// RootEntities contains the list of root entities, which
	// are created when the simulation starts.
	RootEntities []EntitySet `yaml:"root-entities"`
	// Entities contains all named entities in the
	// simulation.
	Entities map[string]Entity `yaml:"entities"`
	// States contains all named states in which entities
	// can be along with transitions between states.
	States map[string]State `yaml:"state"`
	// Backend specifies which backend to use for state
	// transition calls.
	// Possible values are:
	// - http
	// - kafka
	Backend CallBackend `yaml:"backend"`
}

type CallBackend string

var (
	HTTPCallBackend  = CallBackend("http")
	KafkaCallBackend = CallBackend("kafka")
)

type EntitySet struct {
	// Cardinality defines how many of these entities are to be
	// created in the simulation. It may be a fixed value, or value
	// of an attribute.
	Cardinality string `yaml:"cardinality,omitempty"`
	// Entity holds the name of the entity
	Entity string `yaml:"entity"`
	// Timer defines the cadence at which new entity instances are created.
	Timer Timer `yaml:"timer,omitempty"`
}

// Entity holds information about an entity in the simulation.
type Entity struct {
	// Attributes define named values that can be used
	// in the simulation and how these values should be set
	Attributes map[string]Attribute `yaml:"attributes,omitempty"`
	// InitialState names the initial state in which the entity is
	// once it is created.
	InitialState string `yaml:"initial_state,omitempty"`
	// Subordinates names the subordinate entities that are to be
	// created, their cardinality and a timer, which determines
	// when a new subordinate entity is to be created.
	Subordinates []EntitySet `yaml:"subordinates,omitempty"`
}

type State struct {
	// Attributes define named values that can be used in
	// state transitions.
	Attributes map[string]Attribute `yaml:"attributes,omitempty"`
	// Timer defines the cadence at which the state will choose
	// one of the transtitions to perform.
	Timer Timer `yaml:"timer,omitempty"`
	// Transitions holds a list of all transitions from
	// this state into another.
	Transitions []Transition `yaml:"transitions,omitempty"`
}

// TODO ADD NOP CALL

type Transition struct {
	// State contains the name of the state into which this
	// transition leads.
	State string `yaml:"state"`
	// Probability holds the probability of this transition
	Probability float64 `yaml:"probability,omitempty"`
	// Call holds the configuration information for a
	// http requedt to be performed on state transition and
	// instructions on what to do with result
	Call Call `yaml:"call"`
	// OnFailure names the state to which to transition
	// in case of call failure.
	OnFailure string `yaml:"on-failure,omitempty"`
}

type Call struct {
	// Method holds the http method.
	Method string `yaml:"method"`
	// URL contains the URL of the request. It may contain
	// wildcards that name an attribute, e.g.
	// {:base_url:}/entity1
	URL string `yaml:"url"`
	// Parameters holds the specification for http request parameters.
	Parameters []CallParameter `yaml:"params"`
	Results    []CallResult    `yaml:"results"`
}

type CallResult struct {
	// Key holds the name of the JSON field.
	Key string `yaml:"key"`
	// Attribute holds the name of the attribute
	// to be set.
	Attribute string `yaml:"attribute"`
}

type CallParameterType string

var (
	BodyCallParameterType   = CallParameterType("body")
	FormCallParameterType   = CallParameterType("form")
	HeaderCallParameterType = CallParameterType("header")
)

type CallParameter struct {
	// Type determines how a parameter is to be user
	// and may be one of the following:
	// - form: means that the parameter will be encoded as
	//         a query parameter in form <Key>=<value>
	// - body: means that the parameter will be added to
	//         request body as a json encoded field
	// - header: means that the parameter will be added
	//         to the request header
	Type CallParameterType `yaml:"type"`
	// Attribute names the attribute value to be used
	Attribute string `yaml:"attribute"`
	// Key specifies the parameter key.
	Key string `yaml:"key"`
}

type TimerType string

var (
	RandomTimer = TimerType("random")
	FixedTimer  = TimerType("fixed")
)

type Timer struct {
	// Type defines the type of the timer and may be one of the
	// following:
	// - random: means the timer will fire at random time intervals
	//           between Min and Max durations long.
	// - fixed: means the timer will fire at fixed intervals defined
	//          by Interval
	Type     TimerType     `yaml:"type"`
	Min      time.Duration `yaml:"min,omitempty"`
	Max      time.Duration `yaml:"max,omitempty"`
	Interval time.Duration `yaml:"interval,omitempty"`
}

type AttributeType string

var (
	ConstantIntAttributeType    = AttributeType("int")
	MapAttributeType            = AttributeType("map")
	MapListAttributeType        = AttributeType("map-list")
	RandomIntAttributeType      = AttributeType("random_int")
	NormalIntAttributeType      = AttributeType("normal_int")
	PowerIntAttributeType       = AttributeType("power_int")
	RandomFloatAttributeType    = AttributeType("random_float")
	NormalFloatAttributeType    = AttributeType("normal_float")
	PowerFloatAttributeType     = AttributeType("power_float")
	ConstantStringAttributeType = AttributeType("string")
	RandomStringAttributeType   = AttributeType("random_string")
	RandomValueAttributeType    = AttributeType("random_value")
	RandomSubsetAttributeType   = AttributeType("random_subset")
)

// Attribute holds the definition of the attribute's value or
// values that define the distribution from which to sample the
// attribute's value.
type Attribute struct {
	// Type may be on of the following:
	// - constant: means the attribute has the value set in Value
	// - map-list: means the attribute take value from the MapValues
	// - map: means the attribute taks value from the MapValue
	// - random: meaning a random value between Min and Max
	//           should be chosen
	// - power: meaning the sampled valued of the attribute should
	//          follow the power law
	//          See http://mathworld.wolfram.com/RandomNumber.html
	//          The attribute's value is derived as
	//          [(max^(n+1)-min^(n+1))*rand()+min^(n+1)]^(1/(n+1))
	// - normal: meaning the value will be sampled from a normal
	//          distribution with mean N and std deviation StdDev
	// - random_string: meaning a uuid will be created and the
	//          value returned will be
	// - random_value: meaning a random value from the Values
	//          list will be chosen
	// - random_subset: meaning a random subset of Values will
	//          be chosen
	Type        AttributeType       `yaml:"type"`
	StringValue string              `yaml:"string-value,omitempty"`
	Value       float64             `yaml:"value,omitempty"`
	Min         float64             `yaml:"min,omitempty"`
	Max         float64             `yaml:"max,omitempty"`
	N           float64             `yaml:"n,omitempty"`
	StdDev      float64             `yaml:"std-dev,omitempty"`
	Values      []interface{}       `yaml:"values,omitempty"`
	MapValue    map[string]string   `yaml:"map-value,omitempty"`
	MapValues   []map[string]string `yaml:"map-values,omitempty"`
}
