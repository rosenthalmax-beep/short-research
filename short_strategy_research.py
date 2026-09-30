                parity_rows.append({
                    "variant_id": variant, "stage": "FULL_LEDGER", "cost_label": label,
                    "status": "PASS" if ok else "FAIL",
                    "accepted_trades": len(accepted), "expected_accepted_trades": exp["accepted"],
                    "total_r": m["total_r"], "expected_total_r": tgt["total_r"],
                    "profit_factor": m["profit_factor"], "expected_profit_factor": tgt["profit_factor"],
                    "max_drawdown_r": m["max_drawdown_r"], "expected_max_drawdown_r": tgt["max_drawdown_r"],
                    "ledger_sha256": ledger_sha, "expected_ledger_sha256": tgt["ledger_sha256"],
                })
                summary_rows.append({
                    "variant_id": variant, "rr": RR, "cost_label": label,
                    "adverse_ticks": ticks, "adverse_pips": pips, "cost_purpose": purpose,
                    "qualified_raw_signals": len(q), "accepted_trades": len(accepted),
                    "right_censored": censored, **m,
                })
                for seq, r in enumerate(accepted, start=1):
                    ledger_rows.append({
                        "variant_id": variant, "rr": RR, "cost_label": label, "sequence": seq,
                        "signal_time": iso(r["signal_time"]), "exit_time": iso(r["exit_time"]),
                        "signal_index": r["signal_index"], "exit_index": r["exit_index"],
                        "reference_entry": r["reference_entry"], f"historical_fill": r[f"fill__{label}"],
                        "stop": r["stop"], "target": r["target"], "exit_reason": r["exit_reason"],
                        "result_r": r[f"result_r__{label}"],
                    })
                year_rows.extend(calendar_rows(variant, accepted, label))
                periods.extend(period_rows(variant, accepted, label))
                rolling_rows.extend(rolling_summary_rows(variant, accepted, label))

        write_csv(OUTPUTS["final_parity"], parity_rows)
        if any(x["status"] != "PASS" for x in parity_rows):
            raise RuntimeError("Independent final-strategy parity FAILED; do not proceed to portfolio admission")

        write_csv(OUTPUTS["summary"], summary_rows)
        write_csv(OUTPUTS["ledgers"], ledger_rows)
        write_csv(OUTPUTS["years"], year_rows)
        write_csv(OUTPUTS["periods"], periods)
        write_csv(OUTPUTS["rolling"], rolling_rows)
        write_csv(OUTPUTS["methodology"], [
            {"topic": "purpose", "value": "Independent implementation confirmation of frozen AUD/JPY H1 SHORT PRIMARY and QUALITY at RR3.25 before exact Portfolio 30 -> 31 admission."},
            {"topic": "independence", "value": "Standalone scalar reimplementation; does not import/call Pass 1-4 feature, qualification, replay or metric functions. NumPy is used only for the ATR seed mean to preserve floating-history parity."},
            {"topic": "price_source", "value": "OANDA completed MID candles; historical adverse fills are assumptions, not measured historical bid/ask."},
            {"topic": "execution", "value": "SHORT stop=signal high+10 ticks; target=reference close - 3.25*(stop-reference close); adverse fills 10T/20T/40T; p0 half-open [signal,exit) so exit-candle re-entry is eligible."},
            {"topic": "primary_rules", "value": "Exact bearish engulf; previous175 high; abs high distance<=0.10 ATR14; body>=0.60 ATR14; range>=1.25 ATR14; previous H1 close location>=0.50; RR3.25."},
            {"topic": "quality_rules", "value": "PRIMARY plus previous completed D1 ATR14 / previous50 D1 ATR14 mean <=1.15."},
            {"topic": "d1_asof", "value": f"D1 clipped through {iso(EXPECTED_D1_LAST_OPEN)}; daily bar j is available only from the next aligned D1 open."},
            {"topic": "interpretation", "value": "Trade-for-trade confirmation on repeatedly examined history. Passing parity is an implementation gate, not fresh OOS evidence or live authorisation."},
            {"topic": "next_gate", "value": "Only if every source/raw/qualified/accepted/full-ledger parity check passes: run exact Portfolio 30 -> 31 admission independently for PRIMARY and QUALITY; do not use portfolio results to retune either candidate."},
        ])
        pack_results()
        set_status(
            state="complete", progress=100,
            message="AUD/JPY H1 SHORT independent confirmation complete; all parity gates passed",
            parity_passed=True, h1_candles=len(h1), d1_candles=len(d1),
            raw_exact_signals=len(raw), results_zip=str(BUNDLE),
        )
    except Exception as exc:
        tb = traceback.format_exc()
        write_csv(OUTPUTS["errors"], [{"error": repr(exc), "traceback": tb}])
        try:
            pack_results()
        except Exception:
            pass
        set_status(state="error", progress=100, message=str(exc), parity_passed=False, error=repr(exc))

# ============================================================
# FLASK / RAILWAY
# ============================================================

def ensure_started():
    global RUN_STARTED
    with RUN_LOCK:
        if RUN_STARTED:
            return
        RUN_STARTED = True
        t = threading.Thread(target=run_confirmation, name="audjpy-h1-short-pass5", daemon=True)
        t.start()


@app.get("/")
def root():
    ensure_started()
    return jsonify({
        "status": "online", "pass_version": PASS_VERSION,
        "research_only": True, "orders_supported": False,
        "status_route": "/audjpy-h1-short-pass5/status",
        "results_route": "/audjpy-h1-short-pass5/results",
        "frozen_variants": ["PRIMARY", "QUALITY"], "rr": RR,
    })


@app.get("/audjpy-h1-short-pass5/status")
def status_route():
    ensure_started()
    with STATUS_LOCK:
        return jsonify(dict(STATUS))


@app.get("/audjpy-h1-short-pass5/results")
def results_route():
    ensure_started()
    with STATUS_LOCK:
        state = STATUS.get("state")
    if state == "complete" and BUNDLE.exists():
        return send_file(BUNDLE, as_attachment=True, download_name=BUNDLE.name)
    if state == "error" and BUNDLE.exists():
        return send_file(BUNDLE, as_attachment=True, download_name=BUNDLE.name)
    return jsonify(dict(STATUS)), 202


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
