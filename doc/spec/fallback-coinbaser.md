# DATUM Fallback Coinbaser Spec

## Problem

When a new block arrives before the pool answers the coinbaser request, the gateway has no split to pay. Today it mines pool-only or solo, or, with stretch reuse, rescales the last split to the new reward. Stretch reuse can pay a one-time output, such as a minimum payout, a second time. The pool should decide what a fallback split pays, not the gateway.

## Format

There is no separate message. A new 0x11 reply version, chosen at handshake, carries the split as fixed amounts plus shares of the value. Each answer pays the requested value and also serves as the fallback for the next block. The reply is fee-aware, because the 0x10 request already carries the full coinbase value, fees included.

Reply header:

| Field | Size | Meaning |
|---|---|---|
| id | 1 byte | Coinbaser id, unchanged from 0x11 |
| prevhash | 32 bytes | Echo of the 0x10 request's prevhash, which binds the list to a tip |
| default_min | u32 LE | Global threshold in sats, at least 546 |
| flags | 1 byte | Bit 0 renormalizes parts on the next block (rule 2). Other bits are reserved and zero |

Each output:

| Field | Size | Meaning |
|---|---|---|
| control | u16 LE | Bits 0-2 fixed size, bits 3-5 parts size, bits 6-8 min code, bit 9 remainder output, bits 10-15 script_len |
| fixed | 0 to 5 bytes LE | Sats paid only on the reply's own tip (u40 max) |
| parts | 0 to 4 bytes LE | Share of the value in parts per 2^32 |
| min | 0 to 4 bytes LE | Per-output threshold. Min code 0 uses default_min, 7 means none, and 1 to 4 give the field's byte count |
| script | script_len | Output script, 1 to 63 bytes |

A size of 0 means the field is absent and zero. A value may have more than one valid encoding, which is fine because the reply is decoded, never hashed. The reply is capped at 32767 bytes and 512 outputs, the same as 0x11.

At most one output sets bit 9, and that output takes the remainder in rule 4. If none sets it, the remainder output is the pool-script output if the split has one, otherwise the last output.

## Gateway rules

A reply for the current tip and value is used as is. When the pool has not answered in time, the gateway falls back to the latest reply.

1. On the same tip as the reply's prevhash with a new value (fees changed), each output pays fixed + floor(parts × value / 2^32).
2. On the next block, whose parent is the reply's prevhash, fixed amounts are dropped, so one-time payouts are never paid twice. Each output pays floor(parts × value / 2^32), or with flags bit 0 set, floor(parts × value / P), where P is the sum of parts over all outputs, so the whole value is paid out by share. This applies whether or not the pool found the previous block, so the gateway does not need a previous-block check.
3. On any other tip the reply is stale. Mine solo until the pool answers, as in rule 7.
4. Apply amounts in this order. First compute every output's amount by rule 1 or 2. Then drop every output other than the remainder output whose amount is under its threshold or under 546 sats, even with min code 7. The threshold is compared against that output's full amount. Then the remainder output takes value minus the sum of the other outputs, which raises it when outputs were dropped or parts sum short, and lowers it when the amounts add up past the value. If the remainder output ends under its threshold or under 546 sats, the reply is unusable for that value.
5. If the reply is unusable or no output is left, mine solo until the pool answers, as in rule 7.
6. Only a reply from the current session counts. A reconnect clears it.
7. If the pool rejects a fallback share, or no usable reply is held, mine solo until the pool answers for the current tip. Log the reason. Solo needs `pooled_mining_only` off, as today.

This replaces stretch reuse. The order is the pool's reply, then the fallback, then solo.

## Negotiation

The gateway offers DRS hello bit 0x10, and the pool acks it in its handshake selection byte, the same way as 0x04. A pool that acks it switches 0x11 to the new reply version, credits shares on fallback work, and treats a block found on it as a full split. With no ack, both sides keep today's 0x11 and the gateway never falls back to it. When a pool acks both 0x04 and 0x10, 0x10 wins and the gateway does not stretch.

A share on fallback work carries the coinbaser id of the reply it was built from. The pool accepts it when the share's tip is that reply's prevhash or its child, and pays it by the rules above.

A gateway could pick the fallback on purpose after the real answer arrived. Miners keep returning shares on the old job for several seconds after a new one goes out, so the window starts when the gateway has visibly switched. The pool may reject fallback shares for a tip that reach it more than 5 seconds after the session's first share carrying the coinbaser id of its 0x11 answer for that tip and value, or more than 30 seconds after that answer if no such share arrives.

The pool does not push replies unprompted in this version. The gateway already sends a 0x10 request for every new template.

## Test vectors

The reference implementation ships test vectors for rules 1, 2 and 4, each giving reply bytes and a value in and the exact outputs out, so a pool can check that it recomputes the fallback coinbase to the sat.
