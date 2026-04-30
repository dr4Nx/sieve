# Human baseline scripts

Hand-written grep/Python reference scripts representing a typical sysadmin
approach to each query. We use them as the comparison point for LLM-generated
filters in the `human-baseline` experiment.

## Layout

```
human_baselines/
  audit_simple/      9 .sh + 3 .py
  audit_complex/     8 .py
  cron_simple/       9 .sh + 3 .py
  cron_complex/      8 .py
  puppet_simple/     9 .sh + 3 .py
  puppet_complex/    8 .py
  sshd_simple/       9 .sh + 3 .py
  sshd_complex/      8 .py
  dhcp_simple/       22 .sh + 10 .py
  dhcp_complex/      21 .py
```

## Conventions

- Where queries (line filters) use bash one-liners with `grep` or `awk`.
- Select queries (extraction) use small Python with `re` and standard
  collections.
- Complex multi-line queries use Python with simple transaction tracking; they
  do not attempt to handle every edge case (time zones, ID inference, rolling
  windows).
- Each script takes the log file path as its only argument and prints results
  to stdout.

## Running a single script

```bash
bash human_baselines/audit_simple/audit_query_1.sh data/logs/audit
python3 human_baselines/dhcp_complex/multiline_3.py data/logs/dhcp
```

## Running the full baseline

```bash
python3 experiments.py --experiment human-baseline \
    --datasets dhcp_simple dhcp_complex audit_simple audit_complex \
               puppet_simple puppet_complex sshd_simple sshd_complex \
               cron_simple cron_complex
```

The runner writes one report per dataset under
`eval/experiments/human-baseline/<timestamp>/`.

## What the scripts do not handle

These reference scripts do not attempt to handle:

- Word boundaries (`grep "3.0.1"` matches `3.0.10`).
- Year boundaries in syslog-style timestamps.
- Transaction ID inference across process restarts.
- Time zone normalization.
- Multi-permission audit denials counted as one event.
- Numeric output formatting (e.g. float precision).

For some DHCP queries (`vague_*`, `numerical_*`) there is no straightforward
grep-style approach; the corresponding scripts return no matches.
