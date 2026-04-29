# Parser-free log querying

This repository accompanies a study of LLM-generated code as a substitute for
log parsers in security log analysis. Given a natural-language query and a raw
log file, an LLM (Gemini) writes a Python or shell filter that reads the file
and returns the matching lines or extracted fields. The framework scores the
filter against rule-based ground truth.

## Layout

```
log_query/         LLM client, prompt construction, sandboxing, templaters
evaluation/        Evaluator that runs queries, computes F1, writes reports
ground_truth/      Per-log-type parsers and code that builds the query JSONs
queries/           Pre-built query benchmarks (one JSON per dataset)
human_baselines/   Naive grep/Python scripts (the human comparison point)
log_parser.py      Single-query CLI wrapper around log_query
evaluate.py        Multi-query evaluator wrapper around evaluation
experiments.py     Experiment orchestrator (template-compare, prompt-ablation, ...)
```

## Setup

Python 3.10 or newer. Install deps:

```bash
pip install -r requirements.txt
```

Set at least one Gemini API key:

```bash
export GEMINI_API_KEY=...           # required for the standalone Gemini API
export GEMINI_API_KEY_2=...         # optional; up to three keys are rotated
export GEMINI_API_KEY_3=...
```

Alternatively, use Vertex AI:

```bash
gcloud auth application-default login
export USE_VERTEX_AI=1
export VERTEX_PROJECT=<gcp-project>
```

## Datasets

The benchmark targets five log types from the Loghub-derived SecurityLogs
corpus: `audit`, `cron`, `dhcp`, `puppet`, `sshd`. The repository ships only
the query JSONs (under `queries/`) and ground-truth builders. The raw log
files are not included.

Place the raw files at:

```
data/logs/audit
data/logs/cron
data/logs/dhcp
data/logs/puppet
data/logs/sshd
```

(or set `LOG_DIR` to override the directory).

Larger query JSONs are stored gzipped (`*.json.gz`) and are loaded
transparently by `evaluate.py`. Pass either path; the loader falls back to
the `.gz` form if the plain `.json` is missing. To inspect them in a text
editor, run `gunzip -k queries/<file>.json.gz`.

## Single query

```bash
python log_parser.py "Find DHCPACK lines" data/logs/dhcp --language python
```

Add `--templates <path>` for hand-written templates, or `--templater frequency`
or `--templater drain3` to auto-generate templates from the log file.

## Evaluating a benchmark

```bash
python evaluate.py queries/dhcp_simple.json data/logs/dhcp \
    --templater frequency \
    --language python \
    --sample-size 100 \
    --max-workers 8
```

Reports are written under `eval/<dataset>/<run>/` with per-query F1, the
generated code, and the matched lines.

## Experiment orchestrator

`experiments.py` wraps `evaluate.py` for the experiments reported in the
paper. Available experiments:

| name                  | what it does                                              |
| --------------------- | --------------------------------------------------------- |
| `template-compare`    | Compare context strategies (manual, drain3, frequency, random, none) |
| `prompt-ablation`     | Drop each prompt component and measure F1                 |
| `model-compare`       | Compare Gemini models on a fixed strategy                 |
| `language-compare`    | Bash vs Python for the generated filter                   |
| `sample-size`         | Sweep the number of sample lines passed in context        |
| `consistency`         | Run each query N times to measure variance                |
| `human-baseline`      | Compare LLM filters against naive grep/Python scripts     |
| `few-shot`            | Add worked query/code examples to the prompt              |
| `retry-analysis`      | Track how often retries flip a wrong answer to a correct one |
| `cross-log-transfer`  | Use templates from a different log type                   |

Example:

```bash
python experiments.py --experiment template-compare \
    --datasets dhcp_simple dhcp_complex \
    --max-workers 20
```

Outputs land in `eval/experiments/<name>/<timestamp>/`.

## Building queries from scratch

Each `ground_truth/<logtype>/build_queries.py` regenerates the dataset's
JSON from the raw log file and a rule-based parser:

```bash
python -m ground_truth.audit.build_queries  --log-file data/logs/audit
python -m ground_truth.cron.build_queries   --log-file data/logs/cron
python -m ground_truth.puppet.build_queries --log-file data/logs/puppet
python -m ground_truth.sshd.build_queries   --log-file data/logs/sshd
```

DHCP queries are not regenerated from a parser; the JSON in `queries/` is the
canonical version.

## Reproducing the human baseline

See `human_baselines/README.md`.

## Notes on determinism

`--sample-seed <int>` makes the reservoir sampling deterministic. Variance
across runs then reflects only model nondeterminism; in our experiments we
report the mean and std across `N` repeats.
