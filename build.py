"""
build.py — Fetches data from Airtable and writes dashboard_data.json.
Run manually or via GitHub Actions every 3 hours.

Required environment variable: AIRTABLE_PAT (your Airtable Personal Access Token)
"""

import os, json, sys
from datetime import datetime, timezone
from collections import defaultdict
import requests

# ─── CONFIG ───
BASE_ID = "app3O6viECRoBLOfS"
TABLES = {
    "emergencies":    "tblCzxMs64dcC52l1",
    "classifications":"tbldDg7nKtafXOPhE",
    "countries":      "tbl9hnLQsbdcW1rS2",
    "offerings":      "tblcat24vc9WLDZ9G",
    "reach_eo":       "tblx3jU3LKJWSmJnK",
    "partners":       "tblP8MRA7ztZ46Hgh",
    "indicators":     "tbljY9cyeL2LFWSWL",
    "first_service":  "tblXm51F54U9jfgNQ",
    "offer_perf":     "tblPi5jbvnl9op3OW",
}
PAT = os.environ.get("AIRTABLE_PAT", "")
if not PAT:
    print("ERROR: Set AIRTABLE_PAT environment variable"); sys.exit(1)

HEADERS = {"Authorization": f"Bearer {PAT}"}
FISCAL_YEAR = 2026  # Oct 2025 – Sep 2026


# ─── AIRTABLE FETCH ───
def fetch_table(table_id):
    """Fetch all records from an Airtable table, handling pagination."""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    records, offset = [], None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=HEADERS, params=params)
        if r.status_code != 200:
            print(f"  WARNING: HTTP {r.status_code} fetching {table_id}: {r.text[:200]}")
            break
        data = r.json()
        for rec in data.get("records", []):
            rec["fields"]["_id"] = rec["id"]
            records.append(rec["fields"])
        offset = data.get("offset")
        if not offset:
            break
    return records


def fetch_all():
    """Fetch all tables."""
    tables = {}
    for name, tid in TABLES.items():
        print(f"  Fetching {name}...")
        tables[name] = fetch_table(tid)
        print(f"    → {len(tables[name])} records")
    return tables


# ─── HELPERS ───
def parse_date(val):
    if not val:
        return None
    try:
        if "T" in str(val):
            return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return datetime.strptime(str(val)[:10], "%Y-%m-%d")
    except Exception:
        return None


def fmt_date(val):
    d = parse_date(val)
    return d.strftime("%b %d, %Y") if d else None


def days_between(d1, d2):
    a, b = parse_date(d1), parse_date(d2)
    if a and b:
        return abs((b - a).days)
    return None


def fiscal_year(date_val):
    d = parse_date(date_val)
    if not d:
        return None
    return d.year + 1 if d.month >= 10 else d.year


def safe_float(v):
    try:
        return float(v) if v not in (None, "", []) else None
    except Exception:
        return None


def safe_int(v):
    try:
        return int(v) if v not in (None, "", []) else None
    except Exception:
        return None


# ─── PROCESS DATA ───
def process(tables):
    emg = tables["emergencies"]
    cls = tables["classifications"]
    eos = tables["offerings"]
    reach = tables["reach_eo"]
    fs_data = tables["first_service"]
    perf = tables["offer_perf"]
    countries = tables["countries"]

    # Build lookups
    country_map = {c["_id"]: c.get("Country", "") for c in countries}
    eo_map = {e["_id"]: e.get("Emergency Intervention Name", "") for e in eos}

    # Group classifications by Class ID, FY-filtered
    class_by_eid = defaultdict(list)
    for c in cls:
        cid = c.get("Class ID", "")
        fy = fiscal_year(c.get("Classification Issued"))
        if cid and fy == FISCAL_YEAR:
            class_by_eid[cid].append(c)

    stance_rank = {"Red": 4, "Orange": 3, "Yellow": 2, "White": 1}

    # ── Build orange/red emergency records ──
    result = []
    for e in emg:
        eid = e.get("Classification ID", "")
        if not eid:
            continue
        classifications = class_by_eid.get(eid, [])
        if not classifications:
            continue

        # Max stance
        max_stance = max(
            classifications,
            key=lambda c: stance_rank.get(c.get("Stance", ""), 0),
            default={},
        )
        stance = max_stance.get("Stance", "")
        if stance not in ("Orange", "Red"):
            continue

        # Earliest classification date
        dates = [parse_date(c.get("Classification Issued")) for c in classifications]
        dates = [d for d in dates if d]
        earliest = min(dates) if dates else None

        affected = safe_int(e.get("Number affected"))
        budget = safe_float(e.get("Response Budget"))
        gap = safe_float(e.get("Gap in Funding"))
        crf = safe_float(e.get("CRF Allocation"))
        funding = safe_float(e.get("Funding Secured"))
        pct_reached = safe_float(e.get("% of total affected reached"))
        target = safe_int(e.get("Response Target"))
        total_reach = safe_int(e.get("Total cumulative reach"))
        pct_funded = safe_float(e.get("% of Response Plan Budget Funded"))

        earliest_str = earliest.strftime("%Y-%m-%d") if earliest else None
        d_decision = days_between(earliest_str, e.get("Date of Decision to respond"))
        d_msna = days_between(earliest_str, e.get("Date of MSNA Completion"))
        d_plan_sub = days_between(
            earliest_str, e.get("Date of MSNA and Response Plan Submission")
        )
        d_plan_app = days_between(
            earliest_str, e.get("Date of Response plan approval (by ERMT)")
        )
        d_client = safe_float(e.get("Days from First Orange Red to First Client"))

        # SAP criteria
        sap = {
            "plan": e.get("Response plan with justification for subsector", ""),
            "learning": e.get("Learning exercise meets minimum requirements", ""),
            "feedback": e.get("Functioning Feedback Mechanism", ""),
            "feedbackTime": e.get(
                "Feedback responded to within expected timeframe", ""
            ),
            "safeguarding": e.get("80% of  Safeguarding Standards Met", ""),
            "partners": e.get(
                "50% of Partners participating in learning exercise", ""
            ),
        }

        # Resolve country name
        country_name = e.get("Countries", "")
        if isinstance(country_name, list) and country_name:
            country_name = country_map.get(country_name[0], str(country_name[0]))

        # ── Reach by month ──
        class_ids = [c["_id"] for c in classifications]
        monthly = defaultdict(lambda: {"irc": 0, "partner": 0})
        for r in reach:
            rcls = r.get("Classification", "")
            if isinstance(rcls, list):
                rcls = rcls[0] if rcls else ""
            if rcls not in class_ids:
                continue
            rd = parse_date(r.get("Reported Date"))
            if not rd:
                continue
            key = rd.strftime("%b %Y")
            val = safe_float(r.get("Reach reported")) or 0
            who = r.get("Partner or IRC", "")
            if who == "IRC":
                monthly[key]["irc"] += val
            else:
                monthly[key]["partner"] += val
        rbm = [
            {"m": k, "i": int(v["irc"]), "p": int(v["partner"])}
            for k, v in sorted(
                monthly.items(),
                key=lambda x: parse_date(f"01 {x[0]}") or datetime.min,
            )
        ]

        # ── Reach by offering ──
        off_reach = defaultdict(lambda: {"r": 0, "t": 0})
        for r in reach:
            rcls = r.get("Classification", "")
            if isinstance(rcls, list):
                rcls = rcls[0] if rcls else ""
            if rcls not in class_ids:
                continue
            eo_id = r.get("Intervention List", "")
            if isinstance(eo_id, list):
                eo_id = eo_id[0] if eo_id else ""
            eo_name = eo_map.get(eo_id, eo_id)
            val = safe_float(r.get("Reach reported")) or 0
            tgt = safe_float(r.get("Target")) or 0
            off_reach[eo_name]["r"] += val
            if tgt > off_reach[eo_name]["t"]:
                off_reach[eo_name]["t"] = tgt
        rbo = [
            {"n": k, "r": int(v["r"]), "t": int(v["t"]) if v["t"] else None}
            for k, v in off_reach.items()
        ]

        # ── First service by offering ──
        fs_list = []
        for f in fs_data:
            fcls = f.get("Classification", "")
            if isinstance(fcls, list):
                fcls = fcls[0] if fcls else ""
            if fcls not in class_ids:
                continue
            eo_id = f.get("Emergency Offering", "")
            if isinstance(eo_id, list):
                eo_id = eo_id[0] if eo_id else ""
            fs_list.append(
                {
                    "o": eo_map.get(eo_id, eo_id),
                    "d": fmt_date(f.get("Date of First Service")) or "—",
                }
            )

        # ── Offering review ──
        or_list = []
        for p in perf:
            pcls = p.get("Classification", "")
            if isinstance(pcls, list):
                pcls = pcls[0] if pcls else ""
            if pcls not in class_ids:
                continue
            eo_id = p.get("Emergency Offering", "")
            if isinstance(eo_id, list):
                eo_id = eo_id[0] if eo_id else ""
            or_list.append(
                {
                    "o": eo_map.get(eo_id, eo_id),
                    "q": p.get("Quality Assessment", ""),
                    "d": fmt_date(p.get("Date Assessed")) or "",
                }
            )

        # Emergency type from latest classification
        e_type = max_stance.get("Emergency Type", "")

        rec = {
            "id": eid,
            "country": country_name or eid[:2],
            "stance": stance,
            "details": e.get("Emergency_Details", "") or "",
            "affected": affected,
            "budget": budget,
            "gap": gap,
            "crf": crf,
            "reached": (
                round(pct_reached * 100, 2)
                if pct_reached
                else (
                    safe_float(e.get("% reached")) if e.get("% reached") else None
                )
            ),
            "daysClient": int(d_client) if d_client is not None else None,
            "daysDecision": d_decision,
            "daysMSNA": d_msna,
            "daysPlanSub": d_plan_sub,
            "daysPlanApproval": d_plan_app,
            "fundingSecured": funding,
            "target": target,
            "totalReach": total_reach,
            "dateClassified": fmt_date(earliest_str) or "",
            "dateDecision": fmt_date(e.get("Date of Decision to respond")),
            "dateMSNA": fmt_date(e.get("Date of MSNA Completion")),
            "datePlanSub": fmt_date(
                e.get("Date of MSNA and Response Plan Submission")
            ),
            "datePlanApproval": fmt_date(
                e.get("Date of Response plan approval (by ERMT)")
            ),
            "dateFirstClient": fmt_date(e.get("Date of First Client Served")),
            "decision": e.get("Response Decision", "")
            or e.get("Response Decision (Max)", ""),
            "type": e_type,
            "pctFunded": round(pct_funded * 100, 1) if pct_funded else None,
            "link": e.get("Response Plan Link", "") or None,
            "sap": sap,
            "rbm": rbm,
            "rbo": rbo,
            "fs": fs_list,
            "or": or_list,
            "pd": [],
            "qi": [],
        }
        result.append(rec)

    # Sort: Red first, then Orange, then by earliest classification
    result.sort(
        key=lambda x: (-stance_rank.get(x["stance"], 0), x["dateClassified"])
    )

    # ── Compute averages ──
    def avg(field):
        vals = [e[field] for e in result if e[field] is not None]
        return round(sum(vals) / len(vals)) if vals else 0

    avgs = {
        "decision": avg("daysDecision"),
        "msna": avg("daysMSNA"),
        "planSub": avg("daysPlanSub"),
        "planApproval": avg("daysPlanApproval"),
        "client": avg("daysClient"),
    }

    # ── Count EO implementations ──
    eo_counts = {}
    for e in result:
        for f in e["fs"]:
            name = f["o"]
            eo_counts[name] = eo_counts.get(name, 0) + 1

    return result, avgs, eo_counts


# ─── MAIN ───
if __name__ == "__main__":
    print("Fetching Airtable data...")
    tables = fetch_all()
    print("Processing data...")
    emergencies, avgs, eo_counts = process(tables)
    print(
        f"  Found {len(emergencies)} Orange/Red emergencies"
    )
    print(
        f"  Averages: Decision={avgs['decision']}d, MSNA={avgs['msna']}d, "
        f"PlanSub={avgs['planSub']}d, PlanApproval={avgs['planApproval']}d, "
        f"Client={avgs['client']}d"
    )

    # ── Write JSON output ──
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output = {
        "lastUpdated": now,
        "fiscalYear": FISCAL_YEAR,
        "averages": avgs,
        "eoCounts": eo_counts,
        "emergencies": emergencies,
    }

    with open("dashboard_data.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(
        f"Done! dashboard_data.json written "
        f"({os.path.getsize('dashboard_data.json'):,} bytes)"
    )
