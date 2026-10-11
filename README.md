# solseller

[![CI](https://github.com/lowriskbtc/solseller/actions/workflows/ci.yml/badge.svg)](https://github.com/lowriskbtc/solseller/actions/workflows/ci.yml) ![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-3776AB?logo=python&logoColor=white) [![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A self-hosted Solana trading terminal built around Jupiter swaps: paste a token address, buy with SOL, sell a percentage of holdings, and let a server-side take-profit / stop-loss executor manage exits. It includes a paper-trading book, an exact pump.fun bonding-curve simulator, and a pop-out "sell box" window.

> Personal trading software shared as source. It can sign and send real transactions from your wallet. Read the code, start in paper mode, and use a wallet funded with an amount you can afford to lose. Nothing here is financial advice.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                   # wallet key and RPC settings
python jup_trader.py                                   # http://127.0.0.1:5010
```

On Windows, `sellbox.bat` opens the compact always-on-top trade box (`pin_topmost.ps1` keeps it above the browser).

## What is in the repo

| File | Role |
|---|---|
| `jup_trader.py` | Flask server and web UI (`JUP Terminator LIVE.html`, `popout.html`): wallet vault, quotes, buys and sells via Jupiter |
| `tpsl.py` | Take-profit / stop-loss executor. `decide()` is a pure function: confirmation counts distinct price prints (not polls), stale prices reset the streak, unverified cost basis never sells, fires latch until the price re-enters the band |
| `paper.py` | Simulated book with its own journal, for the pop-out box |
| `curve_sim.py` | Exact pump.fun constant-product fill math, including your own price impact |
| `pnl_attrib.py` | Per-lot on-chain PnL attribution |
| `smart_exit.py`, `pyramid.py`, `flow_source.py`, `trade_context.py` | Flow-based exits (paper-enforced), scale-in policy (shadow only), pump.fun trade flow from RPC logs, decision context journaling |
| `kol_source.py` | Ranks wallets from a public leaderboard with a Wilson lower bound so thin lucky records cannot outrank real ones |
| `meme_agent.py`, `llm_router.py` | Optional LLM judge for new tokens (logged in shadow, does not place trades) |
| `evm_*.py`, `wallets_db.py` | EVM copy-trading module and cross-chain wallet registry (loaded at startup, so the `web3` packages are required) |

## Security

- Wallet keys are stored encrypted (`wallets.enc.json`, PBKDF2 + Fernet) and unlocked per session; `.env` and the vault are git-ignored.
- The server binds to `127.0.0.1` and rejects any request whose `Host` is not a local name, which blocks DNS-rebinding attacks from web pages. Set `JUP_ALLOWED_HOSTS` only if you deliberately serve it under another name.
- Trading routes accept JSON bodies only, so ordinary cross-site form posts cannot trigger them.

## Tests

```bash
pip install -r requirements-test.txt
pytest tests
```

63 tests cover the TP/SL trigger logic tick by tick, the bonding-curve math (invariants, fees, chunked exits), wallet ranking, pump.fun event decoding, model-output parsing and the host guard. Every protection was verified by a deliberate mutation that the suite catches (11/11). The tests never import `jup_trader.py` (it loads keys and opens RPC sessions on import) and block the network.
