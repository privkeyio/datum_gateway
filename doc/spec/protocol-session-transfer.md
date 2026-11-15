# DATUM Protocol Session and Transfer Changes

## Block fetch by hash

A block fetch names only the block hash, with no job id. The gateway refuses blocks older than a set age.

## Deterministic ABW

ABW rotates on height changes, so each height's ABW can be derived without stored state, and another pool server can resume a session cleanly. Based on the existing abw_v2 draft.

## Bulk transfers

Bulk transfers keep several fragments in flight instead of one per round trip. Fragments still enter only an empty send buffer, so mining frames are not delayed.

## Extranonce

Drop the extranonce field from the coinbase, since it is always null today. Expose the full extranonce space in share submissions. Some bytes are forced to zero today because the protocol has no way to send them.

The pool assigns each session a prefix at handshake, and the gateway puts it in every coinbase it builds for that session. Two gateways can then never build the same work, even with the same config and payout address, so a configured unique ID is no longer needed for that.

## Session takeover

A pool server that takes over a session does not resume the old server's work. It assigns a new prefix and the gateway sends new jobs under it, so no work built for the old server can be submitted to the new one, and the new server needs none of the old server's duplicate state. Shares still queued for replay to the old server are dropped. That costs at most the shares in flight at the moment of transfer.

## Open questions

- What age cap applies to block fetches by hash?
- How is each height's ABW derived (pool key, seed, height), so a second pool server can compute it?
