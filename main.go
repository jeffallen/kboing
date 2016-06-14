// kboing is a synthetic load generator for Kafka clusters.
//
// Each kboing listens to some number of the "boingN" topics. For
// each received message, kboing decreases the TTL. If the new TTL is
// zero, the message dies. Otherwise, the message is either passed to
// the next topic (boing8 -> boing9 -> boing0), or it is flooded to
// all topics, depending on a roll of the dice.
//
// Running "kboing -start" will inject one message with the given TTL into
// topic boing0, to get the echo chamber ringing.
//
// At present, partitions are not used. The key for all messages is nil.
package main

import (
	crand "crypto/rand"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"

	"gopkg.in/mgo.v2/bson"

	"github.com/Shopify/sarama"
)

// Flags
var start = flag.Bool("start", false, "Send one message to to boing0.")
var ncons = flag.Int("ncons", 3, "Number of consumers.")
var floodPct = flag.Int("flood", 0, "Percent of messages to flood.")
var ttl = flag.Int("ttl", 50, "TTL to use on new messages.")
var debugFlag = flag.Bool("debug", false, "Debug?")

var debug = log.New(ioutil.Discard, "", log.LstdFlags)

type slist []string

func (a *slist) String() string {
	return strings.Join(*a, ", ")
}

func (a *slist) Set(in string) error {
	*a = append(*a, in)
	return nil
}

// A Message is the useless data sent through the tubes to generate load.
type Message struct {
	TTL     int      // Time to live. Zero means do not send onwards.
	Trace   []string // A list of all the topics this message has visited.
	encoded []byte   // A cache of the last encoded version. Invalide with Dirty.
}

// Dirty marks the cached encoding as unusable.
func (m *Message) Dirty() {
	m.encoded = nil
}

// Encode implements sarama.Encoder.
func (m *Message) Encode() ([]byte, error) {
	if m.encoded != nil {
		return m.encoded, nil
	}

	// To set m.encoded
	_ = m.Length()

	return m.encoded, nil
}

// Length implements sarama.Encoder.
func (m *Message) Length() int {
	if m.encoded == nil {
		var err error
		m.encoded, err = bson.Marshal(m)
		if err != nil {
			// Should not be possible.
			panic(err)
		}
	}
	return len(m.encoded)
}

// String implements fmt.Stringer
func (m *Message) String() string {
	return fmt.Sprintf("Message{ TTL: %v, Trace: %v }", m.TTL,
		strings.Join(m.Trace, "/"))
}

var allTopics []string

func init() {
	allTopics = make([]string, 10)
	for i := 0; i < 10; i++ {
		allTopics[i] = fmt.Sprintf("boing%v", i)
	}
}

func main() {
	var addrs slist
	flag.Var(&addrs, "addr", "Address(es) of the broker(s).")
	flag.Parse()

	if addrs == nil {
		addrs = []string{"localhost:9092"}
	}

	if *debugFlag {
		debug.SetOutput(os.Stderr)
	}

	var seed [1]byte
	_, err := crand.Read(seed[:])
	if err != nil {
		log.Fatalln("cannot seed")
	}
	rand.Seed(int64(seed[0]))

	prod, err := sarama.NewAsyncProducer(addrs, nil)
	if err != nil {
		log.Fatalln("could not make producer:", err)
	}
	defer func() {
		if err := prod.Close(); err != nil {
			log.Fatalln("close producer:", err)
		}
	}()

	if *start {
		msg := &Message{TTL: *ttl}
		prod.Input() <- &sarama.ProducerMessage{
			Topic: "boing0",
			Key:   nil,
			Value: msg,
		}

		return
	}

	cons, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		log.Fatalln("could not make consumer")
	}
	defer func() {
		if err := cons.Close(); err != nil {
			log.Fatalln("close consumer:", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(*ncons)
	base := int(rand.Int31n(10))
	for i := 0; i < *ncons; i++ {
		go consume(&wg, prod, cons, (base+i)%10)
	}
	wg.Wait()
}

func consume(wg *sync.WaitGroup, prod sarama.AsyncProducer, cons sarama.Consumer, n int) {
	topic := fmt.Sprintf("boing%v", n)
	next := fmt.Sprintf("boing%v", (n+1)%10)

	debug.Println("consume", topic)
	pc, err := cons.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Println("cannot consume topic", topic, ":", err)
		return
	}

	defer func() {
		if err := pc.Close(); err != nil {
			log.Println("consume topic", topic, ":", err)
		}
	}()

	for m := range pc.Messages() {
		var msg Message
		err := bson.Unmarshal(m.Value, &msg)
		if err != nil {
			log.Println("consume topic", topic, ":", err, "(skipping)")
			continue
		}

		//debug.Println("message: ", msg.String())

		if msg.TTL == 0 {
			continue
		}

		msg.TTL--
		msg.Trace = append(msg.Trace, topic)
		msg.Dirty()

		if flood() {
			for i := 0; i < 10; i++ {
				msg.TTL = *ttl
				prod.Input() <- &sarama.ProducerMessage{
					Topic: allTopics[i],
					Key:   nil,
					Value: &msg,
				}
			}
		} else {
			debug.Println("ttl", msg.TTL, "pass to", next)
			prod.Input() <- &sarama.ProducerMessage{
				Topic: next,
				Key:   nil,
				Value: &msg,
			}
		}
	}

	wg.Done()
	return
}

func flood() bool {
	return int(rand.Int31n(100)) < *floodPct
}
