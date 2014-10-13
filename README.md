[![Build Status](https://travis-ci.org/lindenbaum/lbm_nomq.png?branch=master)](https://travis-ci.org/lindenbaum/lbm_nomq)

lbm_nomq
========

An implementation of a reliable message queue mechanism for the use in Erlang
clusters (using Erlang distribution).

`lbm_nomq` is built around the idea to have reliable, topic based message
passing of Erlang terms. When it comes to reliable message passing, messages are
safest when they reside in the originator until they have been successfully
processed by a subscriber. While introducing broker processes is a nice way to
speed up the pushing side, it also raises the danger of loosing every single
message when the broker process exits. So, the worst place for unprocessed
messages is the message queue of a process that is not the message originator.

How it works
------------

To know if a message was handled successfully, an `lbm_nomq:push` is always a
synchronous operation. The semantic and mechanism of `lbm_nomq:push` is very
similar to `gen_server:call/3`.

So why not use `gen_server:call/3` directly? Of course, `lbm_nomq` is not only
a wrapper around `gen_server:call/3`. Furthermore, its `gen_server:call/3` on
steroids with distributed topic subscriber management, load balancing and
failover management in case of multiple subscribers and blocking wait of pushers
if no subscribers are available.

`lbm_nomq` provides total location transparency for pushers and subscribers.
Neither pushers nor subscribers know where the sender/receiver of a message is
located. A pusher does not care if a subscriber fails in the middle of message
processing, as long as there are other subscribers for the respective topic. It
is also transparent how many subscribers a certain topic has. There's no
unsubscribe in `lbm_nomq`. Dead or not available subscribers will be sorted out
automatically when a push fails.

Subscribers even don't have to be processes. A subscriber can be any `MFA` that
adheres the `gen:call` semantic (return on successful message handling or exit
the calling process).

When to use?
------------

Of course, `lbm_nomq` is not general purpose, it is designed to give you
reliable message passing by blocking senders until the message has successfully
been consumed. Therefore, `lbm_nomq` is best suited to be used by many,
concurrent pushing processes in combination with few subscribers per topic.
