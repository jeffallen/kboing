# kboing
A load generator for Kafka

This is a work in (non-)progress. It currently is capable of making short
spikes of work, or generating a runaway torrent or work, but nothing in between.

I am not currently working on this, as the original goal I had (learn
Kafka client programming) has been achieved. I've just checked in what I had
in order to not lose it, in case I want to come back to this later.

To use, run one or more kboings, such that all the 10 topics are covered. For
example:

	./kboing -ncons 7
	./kboing -ncons 7 -flood 1
	# These two will by definition cover all 10 topics, because 7 > 5.

Then run kboing in one-shot mode, to fire in a stimulus to start messages
boinging around.

	./kboing -start

To trigger an exponential explosion of messages, use -flood with a percentage
of 3 or so.