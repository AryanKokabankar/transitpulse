"""Chaos experiments: scripted failures with their conclusions measured.

Every claim this project makes about resilience -- the queue absorbs a
backlog, nothing is dropped silently, the breaker stops calling a dead
upstream, the idempotency key makes a replay free -- is a claim about
behaviour under a fault that does not occur on a healthy day. A README can
assert any of them. The only way to know is to cause the fault and count what
came out the other side.

So each scenario here is a committed script rather than a write-up, and each
one ends in numbers with an assertion attached. The design rules are the same
three the load harness follows:

* **Counted, not asserted rhetorically.** "Zero loss" means
  ``rows == published``, read from Postgres after the fact, with both numbers
  in the result file.
* **Isolated by identity.** Each run invents its own agency, so every count is
  of rows that run created and nothing else on the local stack can perturb
  them. The agency is part of the idempotency key, so the keys are fresh too.
* **The result file is the evidence.** ``docs/benchmarks/*.json`` holds the raw
  measurements, written by the run, so a number in ``BENCHMARKS.md`` can be
  traced to the command that produced it.

    python tasks.py chaos -- --list
    python tasks.py chaos -- kill-processor
    python tasks.py chaos -- all
"""
