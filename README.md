# rust-ln-sync

This is a server that connects to peers on the Lightning network and calculates compact rapid sync
gossip data.

These are the components it's comprised of.

## config

A config file where the Postgres credentials and Lightning peers can be adjusted.

## download

The module responsible for initiating the scraping of the network graph from its peers.

## persistence

The module responsible for persisting all the downloaded graph data to Postgres.

## server

The server is responsible for returning dynamic and snapshotted rapid sync data.

Dynamic sync data is fed a timestamp of the last sync, and it dynamically calculates a delta
such that a minimal change set is returned based on changes which are assumed to have been seen
by the client (ignoring any intermediate changes). Dynamic sync is only available after the first
full graph sync completes on startup.

Snapshot sync data is also based on a timestamp, but unlike dynamic sync, its responses are
precalculated, which is done in a way that considers the possibility that the client may have
intermittently seen later updates.

### snapshot

The snapshotting module is responsible for calculating and storing snapshots. It's started up
as soon as the first full graph sync completes, and then keeps updating the snapshots at a
24-hour-interval.

### lookup

The lookup module is responsible for fetching the latest data from the network graph and Postgres,
and reconciling it into an actionable delta set that the server can return in a serialized format.

It works by collecting all the channels that are currently in the network graph, and gathering
announcements as well as updates for each of them. For the updates specifically, the last update
seen prior to the given timestamp, the latest known updates, and, if necessary, all intermediate
updates are collected.

Then, any channel that has only had an announcement but never an update is dropped. Additionally,
every channel whose first update was seen after the given timestamp is collected alongside its
announcement.

Finally, all channel update transitions are evaluated and collected into either a full or an
incremental update.
