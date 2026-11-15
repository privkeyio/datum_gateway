# DATUM Protocol Session and Transfer Changes

## Block fetch by hash

A block fetch names only the block hash, with no job id. The gateway refuses blocks older than a set age.

## Deterministic ABW

ABW rotates on height changes, so each height's ABW can be derived without stored state, and another pool server can resume a session cleanly. Based on the existing abw_v2 draft.

## Bulk transfers

Bulk transfers keep several fragments in flight instead of one per round trip. Fragments still enter only an empty send buffer, so mining frames are not delayed.

## Extranonce

Drop the extranonce field from the coinbase, since it is always null today. Expose the full extranonce space in share submissions. Some bytes are forced to zero today because the protocol has no way to send them.

## Open questions

- What age cap applies to block fetches by hash?
- How does a pool server that takes over a session detect duplicate shares without the old server's state?
- How is each height's ABW derived (pool key, seed, height), so a second pool server can compute it?
