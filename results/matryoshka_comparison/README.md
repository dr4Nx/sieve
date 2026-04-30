# Matryoshka structured-parsing comparison

We compared queries run over Matryoshka-generated parsers (offline, LLM-built)
against the same queries answered by the LLM-generated code that this
repository produces. Five simple `where` queries per log type were sampled
(seed 27), translated into queries over the parsed representation, and scored
against the same ground truth used by the repository's evaluator.

## Files

- `summary.json`, `summary.md` — aggregate macro precision and recall per
  log type.
- `audit.json`, `cron.json`, `puppet.json`, `sshd.json` — per-query
  precision/recall and the matched line IDs returned by Matryoshka. The
  DHCP detail file is not included; per-query detail for DHCP is in
  `summary.json`.

The numbers in `summary.md` are the Matryoshka side of the comparison
reported in the paper's structured-parsing comparison table.
