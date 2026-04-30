We evaluated Matryoshka-generated parsers by sampling 5 random simple `where` queries per log type from the Sieve query set, translating those queries to run over the Gemini-2.5-Pro-generated Matryoshka parsers, executing them on the parsed logs, and comparing the matched lines against Sieve ground truth. Comparison was done after whitespace normalization so equivalent lines were matched reliably.

On the sampled queries, performance was near-perfect overall. The aggregate macro precision was `0.9997777488597574` and macro recall was `0.9980880356846565`. Per log, the results were:

- `audit`: precision `0.9998862505332007`, recall `0.9955355937673792`
- `cron`: precision `1.0`, recall `1.0`
- `dhcp`: precision `1.0`, recall `1.0`
- `puppet`: precision `1.0`, recall `0.9959595959595958`
- `sshd`: precision `0.999002493765586`, recall `0.9989449886963074`

These results indicate that the translated Matryoshka queries recover the Sieve ground truth with essentially perfect precision and very high recall across the sampled simple filtering queries.
