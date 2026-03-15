"""
build.py — Fetches data from Airtable and writes dashboard_data.json.
Run manually or via GitHub Actions every 3 hours.

Required environment variable: AIRTABLE_PAT (your Airtable Personal Access Token)

Data model (from PowerBI):
  Emergency_Query1 is the central fact table.
  Related tables link via Classification → Emergency._airtableRecordId
  (NOT to the Classifications table — "Classification" is a misnomer in Airtable).
  Classifications link via Class ID → Emergency.Classification ID.
"""

import os, json, sys
from datetime import datetime, timezone
from collections import defaultdict
import requests

# ─── CONFIG ───
BASE_ID = "app3O6viECRoBLOfS"
TABLES = {
    # Core tables (already fetched)
    "emergencies":       "tblCzxMs64dcC52l1",
    "classifications":   "tbldDg7nKtafXOPhE",
    "countries":         "tbl9hnLQsbdcW1rS2",
    "offerings":         "tblcat24vc9WLDZ9G",
    "reach_eo":          "tblx3jU3LKJWSmJnK",
    "partners":          "tblP8MRA7ztZ46Hgh",
    "indicators":        "tbljY9cyeL2LFWSWL",
    "first_service":     "tblXm51F54U9jfgNQ",
    "offer_perf":        "tblPi5jbvnl9op3OW",
    # Additional tables discovered from PowerBI model
    "reach_sub":         "tblx3jU3LKJWSmJnK",  # placeholder — update with real ID
    "indicator_report":  "tbljY9cyeL2LFWSWL",  # placeholder — update with real ID
    "subsector_perf":    "tblPi5jbvnl9op3OW",  # placeholder — update with real ID
    "interventions":     "tblcat24vc9WLDZ9G",  # placeholder — update with real ID
    "subsectors":        "tbl9hnLQsbdcW1rS2",  # placeholder — update with real ID
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
    """Fetch all tables (deduplicating by table ID)."""
    tables = {}
    fetched_ids = {}
    for name, tid in TABLES.items():
        if tid in fetched_ids:
            # Same table ID already fetched under another name — reuse it
            tables[name] = tables[fetched_ids[tid]]
            print(f"  {name}: reusing {fetched_ids[tid]} ({len(tables[name])} records)")
            continue
        print(f"  Fetching {name}...")
        tables[name] = fetch_table(tid)
        fetched_ids[tid] = name
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


def resolve_link(val):
    """Extract first value from an Airtable linked-record field (list of IDs)."""
    if isinstance(val, list):
        return val[0] if val else ""
    return val or ""


# ─── PROCESS DATA ───
def process(tables):
    emg = tables["emergencies"]
    cls = tables["classifications"]
    eos = tables["offerings"]
    reach = tables["reach_eo"]
    fs_data = tables["first_service"]
    perf = tables["offer_perf"]
    countries = tables["countries"]
    partners_data = tables["partners"]
    indicators_data = tables["indicators"]

    # ── DIAGNOSTIC: dump field names from key tables ──
    print("\n── DIAGNOSTICS ──")
    for tname, tdata in [
        ("Emergencies", emg), ("Classifications", cls), ("Offerings", eos),
        ("Reach_EO", reach), ("First_Service", fs_data), ("Offer_Perf", perf),
        ("Partners", partners_data), ("Indicators", indicators_data),
    ]:
        print(f"  {tname}: {len(tdata)} records")
        if tdata:
            print(f"    Fields: {list(tdata[0].keys())}")

    # ── Build lookups ──
    country_map = {c["_id"]: c.get("Country", "") for c in countries}
    eo_map = {e["_id"]: e.get("Emergency Intervention Name", "") for e in eos}

    # ── Group classifications by Class ID ──
    # Classifications link to emergencies via: Class ID → Emergency.Classification ID
    class_by_eid = defaultdict(list)
    for c in cls:
        cid = c.get("Class ID", "")
        if cid:
            class_by_eid[cid].append(c)

    print(f"\n  Classifications grouped by Class ID: {len(class_by_eid)} unique IDs")

    # ── KEY INSIGHT from PowerBI model: ──
    # Related tables (reach, first_service, partners, offer_perf, indicator_report)
    # link via their "Classification" field to Emergency._airtableRecordId (the
    # emergency's Airtable record ID), NOT to classification record IDs.
    #
    # So we must index these tables by the EMERGENCY record ID.

    def build_index_by_emergency(records, link_field="Classification"):
        """Index records by emergency _airtableRecordId (via the 'Classification' link field)."""
        by_emg = defaultdict(list)
        for r in records:
            link_val = r.get(link_field, "")
            linked_ids = link_val if isinstance(link_val, list) else ([link_val] if link_val else [])
            for lid in linked_ids:
                by_emg[lid].append(r)
        return by_emg

    reach_by_emg = build_index_by_emergency(reach)
    fs_by_emg = build_index_by_emergency(fs_data)
    perf_by_emg = build_index_by_emergency(perf)
    partner_by_emg = build_index_by_emergency(partners_data)
    indicator_by_emg = build_index_by_emergency(indicators_data)

    print(f"\n  Reach indexed by emergency ID: {len(reach_by_emg)} emergencies with reach data")
    print(f"  First_Service indexed: {len(fs_by_emg)} emergencies with FS data")
    print(f"  Offer_Perf indexed: {len(perf_by_emg)} emergencies with perf data")
    print(f"  Partners indexed: {len(partner_by_emg)} emergencies with partner data")
    print(f"  Indicators indexed: {len(indicator_by_emg)} emergencies with indicator data")

    # Show sample linking data for verification
    if reach:
        sample = reach[0]
        print(f"\n  Sample reach 'Classification' field value: {sample.get('Classification', 'MISSING')}")
    if emg:
        sample = emg[0]
        print(f"  Sample emergency '_id' (record ID): {sample.get('_id', 'MISSING')}")

    stance_rank = {"Red": 4, "Orange": 3, "Yellow": 2, "White": 1}

    # ── Build orange/red emergency records ──
    result = []
    debug_counts = {"no_eid": 0, "no_classifications": 0, "not_orange_red": 0, "wrong_fy": 0, "matched": 0}
    debug_program_data = {"reach": 0, "fs": 0, "perf": 0, "partner": 0, "indicator": 0}

    for e in emg:
        eid = e.get("Classification ID", "")
        emg_record_id = e.get("_id", "")  # Airtable record ID — used for program data linking
        if not eid:
            debug_counts["no_eid"] += 1
            continue
        classifications = class_by_eid.get(eid, [])
        if not classifications:
            debug_counts["no_classifications"] += 1
            continue

        # FY filter using emergency-level "Earliest Classification" or "Start Date"
        earliest_date_str = e.get("Earliest Classification") or e.get("Start Date")
        fy = fiscal_year(earliest_date_str)
        if fy != FISCAL_YEAR:
            debug_counts["wrong_fy"] += 1
            continue

        # Max stance
        max_stance = max(
            classifications,
            key=lambda c: stance_rank.get(c.get("Stance", ""), 0),
            default={},
        )
        stance = max_stance.get("Stance", "")
        if stance not in ("Orange", "Red"):
            debug_counts["not_orange_red"] += 1
            continue
        debug_counts["matched"] += 1

        # Earliest classification date
        earliest = parse_date(earliest_date_str)

        affected = safe_int(e.get("Number affected"))
        budget = safe_float(e.get("Response Budget"))
        gap = safe_float(e.get("Gap in Funding"))
        crf = safe_float(e.get("CRF Allocation"))
        funding = safe_float(e.get("Funding Secured"))
        appeal = safe_float(e.get("Appeal Allocation"))
        pct_reached = safe_float(e.get("% of total affected reached"))
        target = safe_int(e.get("Response Target"))
        total_reach_raw = e.get("Total cumulative reach")
        total_reach = safe_int(total_reach_raw)
        pct_funded = safe_float(e.get("% of Response Plan Budget Funded"))
        pct_prep = safe_float(e.get("% of Preparedness Checklist Completed"))
        total_in_op_area = safe_int(e.get("Total Affected Population in IRC Operational Areas"))
        reach_6mo_class = safe_int(e.get("Total Number Reached 6 months from classification"))
        reach_6mo_svc = safe_int(e.get("Total Reach 6 months after first client served"))
        pct_10_target_6mo = safe_float(e.get("% of 10% target reached 6 months from first service"))
        pct_reach_op_area = safe_float(e.get("% reach of total affected in operational area"))
        ten_pct_affected = safe_float(e.get("10% of number affected"))

        earliest_str = earliest.strftime("%Y-%m-%d") if earliest else None
        d_decision = days_between(earliest_str, e.get("Date of Decision to respond"))
        d_msna = days_between(earliest_str, e.get("Date of MSNA Completion"))
        d_msna_start = days_between(earliest_str, e.get("Date of MSNA Data Collection Started"))
        d_plan_sub = days_between(
            earliest_str, e.get("Date of MSNA and Response Plan Submission")
        )
        d_plan_app = days_between(
            earliest_str, e.get("Date of Response plan approval (by ERMT)")
        )
        d_client = safe_float(e.get("Days from First Orange Red to First Client"))
        d_disb_to_sig = safe_float(e.get("Average Time from disbursement to agreement signature"))
        d_sig_to_receipt = safe_float(e.get("Average Time from agreement signature to partner receipt of funds"))

        # SAP criteria
        sap = {
            "plan": e.get("Response plan with justification for subsector", ""),
            "learning": e.get("Learning exercise meets minimum requirements", ""),
            "feedback": e.get("Functioning Feedback Mechanism", ""),
            "feedbackTime": e.get("Feedback responded to within expected timeframe", ""),
            "safeguarding": e.get("80% of  Safeguarding Standards Met", ""),
            "partners": e.get("50% of Partners participating in learning exercise", ""),
            "indicators80": e.get("80% of indicators on track", ""),
            "sequenced": e.get("Response plan with sequenced emergency outcome programming", ""),
            "msnaCompleted": e.get("Was an MSNA completed?", ""),
            "localSystems": e.get("Analysis of local systems and capacities", ""),
        }

        # Resolve country name
        country_name = e.get("Countries", "")
        if isinstance(country_name, list) and country_name:
            country_name = country_map.get(country_name[0], str(country_name[0]))

        # ── Collect related records via emergency record ID ──
        related_reach = reach_by_emg.get(emg_record_id, [])
        related_fs = fs_by_emg.get(emg_record_id, [])
        related_perf = perf_by_emg.get(emg_record_id, [])
        related_partners = partner_by_emg.get(emg_record_id, [])
        related_indicators = indicator_by_emg.get(emg_record_id, [])

        if related_reach: debug_program_data["reach"] += 1
        if related_fs: debug_program_data["fs"] += 1
        if related_perf: debug_program_data["perf"] += 1
        if related_partners: debug_program_data["partner"] += 1
        if related_indicators: debug_program_data["indicator"] += 1

        # ── Reach by month ──
        monthly = defaultdict(lambda: {"irc": 0, "partner": 0})
        for r in related_reach:
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
        for r in related_reach:
            eo_id = resolve_link(r.get("Intervention List", ""))
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
        for f in related_fs:
            eo_id = resolve_link(f.get("Emergency Offering", ""))
            fs_list.append({
                "o": eo_map.get(eo_id, eo_id),
                "d": fmt_date(f.get("Date of First Service")) or "—",
                "type": f.get("Service type", "") or "",
            })

        # ── Offering review / performance ──
        or_list = []
        for p in related_perf:
            eo_id = resolve_link(p.get("Emergency Offering", ""))
            or_list.append({
                "o": eo_map.get(eo_id, eo_id),
                "q": p.get("Quality Assessment", ""),
                "d": fmt_date(p.get("Date Assessed")) or "",
            })

        # ── Partner data ──
        pd_list = []
        for p in related_partners:
            eo_id = resolve_link(p.get("Emergency Offering Implemented", ""))
            pd_list.append({
                "partner": p.get("Partner", ""),
                "offering": eo_map.get(eo_id, eo_id),
                "en": p.get("Existing Partner or New Partner", ""),
                "disb": fmt_date(p.get("Date of first disbursement")),
                "fs": fmt_date(p.get("Date of First service")),
                "fd": fmt_date(p.get("Funding delivery date")),
                "agreement": fmt_date(p.get("Date Partnership Agreement signed or updated")),
                "disbToSig": p.get("Time from disbursement to agreement signature", ""),
                "sigToReceipt": p.get("Time from partnership agreement signature to partner recipt of funds", ""),
            })

        # ── Quality indicators / indicator reporting ──
        qi_list = []
        for i in related_indicators:
            ind_id = resolve_link(i.get("Indicator", ""))
            qi_list.append({
                "o": i.get("Name", "") or ind_id,
                "v": i.get("Data Value", ""),
                "t": i.get("Target", ""),
                "d": fmt_date(i.get("Reporting Date")),
            })

        # Emergency type from latest classification
        e_type = max_stance.get("Emergency Type", "")
        severity = max_stance.get("Severity", "")
        region = max_stance.get("IRC Region", "")

        rec = {
            "id": eid,
            "emgRecordId": emg_record_id,
            "country": country_name or eid[:2],
            "region": region,
            "stance": stance,
            "severity": severity,
            "details": e.get("Emergency_Details", "") or "",
            "affected": affected,
            "budget": budget,
            "gap": gap,
            "crf": crf,
            "appeal": appeal,
            "reached": (
                round(pct_reached * 100, 2) if pct_reached
                else (safe_float(e.get("% reached")) if e.get("% reached") else None)
            ),
            "daysClient": int(d_client) if d_client is not None else None,
            "daysDecision": d_decision,
            "daysMSNA": d_msna,
            "daysMSNAStart": d_msna_start,
            "daysPlanSub": d_plan_sub,
            "daysPlanApproval": d_plan_app,
            "avgDisbToSig": round(d_disb_to_sig, 1) if d_disb_to_sig is not None else None,
            "avgSigToReceipt": round(d_sig_to_receipt, 1) if d_sig_to_receipt is not None else None,
            "fundingSecured": funding,
            "target": target,
            "totalReach": total_reach,
            "totalInOpArea": total_in_op_area,
            "reach6moClass": reach_6mo_class,
            "reach6moSvc": reach_6mo_svc,
            "pct10Target6mo": round(pct_10_target_6mo * 100, 1) if pct_10_target_6mo else None,
            "pctReachOpArea": round(pct_reach_op_area * 100, 2) if pct_reach_op_area else None,
            "tenPctAffected": ten_pct_affected,
            "pctPrep": round(pct_prep * 100, 1) if pct_prep else None,
            "dateClassified": fmt_date(earliest_str) or "",
            "dateDecision": fmt_date(e.get("Date of Decision to respond")),
            "dateMSNAStart": fmt_date(e.get("Date of MSNA Data Collection Started")),
            "dateMSNA": fmt_date(e.get("Date of MSNA Completion")),
            "datePlanSub": fmt_date(e.get("Date of MSNA and Response Plan Submission")),
            "datePlanApproval": fmt_date(e.get("Date of Response plan approval (by ERMT)")),
            "dateFirstClient": fmt_date(e.get("Date of First Client Served")),
            "dateFirstCash": fmt_date(e.get("Date of First Cash Distribution")),
            "date6moMark": fmt_date(e.get("6 Month Reporting Mark")),
            "date3moMark": fmt_date(e.get("3 Month Mark")),
            "dateLastReach": fmt_date(e.get("Last Updated Reach")),
            "decision": e.get("Response Decision", "") or e.get("Response Decision (Max)", ""),
            "ermtDecision": e.get("ERMT Response Decision", ""),
            "type": e_type,
            "pctFunded": round(pct_funded * 100, 1) if pct_funded else None,
            "link": e.get("Response Plan Link", "") or None,
            "firstServiceType": e.get("First Service Type", ""),
            "newOrExisting": e.get("New Location Or Existing", ""),
            "sapStartTriggered": e.get("SAP Reporting Start Triggered", ""),
            "sapMidTriggered": e.get("SAP Reporting Midpoint triggered", ""),
            "sapEndTriggered": e.get("SAP Reporting End triggered", ""),
            "sap": sap,
            "rbm": rbm,
            "rbo": rbo,
            "fs": fs_list,
            "or": or_list,
            "pd": pd_list,
            "qi": qi_list,
        }
        result.append(rec)

    print(f"\n  Emergency filtering:")
    print(f"    No Classification ID: {debug_counts['no_eid']}")
    print(f"    No matching classifications: {debug_counts['no_classifications']}")
    print(f"    Wrong FY: {debug_counts['wrong_fy']}")
    print(f"    Not Orange/Red: {debug_counts['not_orange_red']}")
    print(f"    MATCHED (Orange/Red): {debug_counts['matched']}")

    print(f"\n  Program data matching (out of {debug_counts['matched']} emergencies):")
    print(f"    With reach data: {debug_program_data['reach']}")
    print(f"    With first_service data: {debug_program_data['fs']}")
    print(f"    With offer_perf data: {debug_program_data['perf']}")
    print(f"    With partner data: {debug_program_data['partner']}")
    print(f"    With indicator data: {debug_program_data['indicator']}")

    # Show sample matched emergency
    if result:
        s = result[0]
        print(f"\n  Sample emergency '{s['id']}' ({s['country']}):")
        print(f"    rbm={len(s['rbm'])}, rbo={len(s['rbo'])}, fs={len(s['fs'])}, "
              f"or={len(s['or'])}, pd={len(s['pd'])}, qi={len(s['qi'])}")

    print("── END DIAGNOSTICS ──\n")

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
    # Count from first_service (most reliable source)
    for e in result:
        for f in e["fs"]:
            name = f["o"]
            if name:
                eo_counts[name] = eo_counts.get(name, 0) + 1
    # Fallback: count from reach-by-offering if no FS data
    if not eo_counts:
        for e in result:
            seen = set()
            for r in e["rbo"]:
                name = r["n"]
                if name and name not in seen:
                    seen.add(name)
                    eo_counts[name] = eo_counts.get(name, 0) + 1
    # Last fallback: count from offering performance
    if not eo_counts:
        for e in result:
            for o in e["or"]:
                name = o["o"]
                if name:
                    eo_counts[name] = eo_counts.get(name, 0) + 1

    return result, avgs, eo_counts


# ─── MAIN ───
if __name__ == "__main__":
    print("Fetching Airtable data...")
    tables = fetch_all()
    print("Processing data...")
    emergencies, avgs, eo_counts = process(tables)
    print(f"  Found {len(emergencies)} Orange/Red emergencies")
    print(f"  Averages: Decision={avgs['decision']}d, MSNA={avgs['msna']}d, "
          f"PlanSub={avgs['planSub']}d, PlanApproval={avgs['planApproval']}d, "
          f"Client={avgs['client']}d")
    print(f"  EO Counts: {json.dumps(eo_counts)}")

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

    print(f"Done! dashboard_data.json written ({os.path.getsize('dashboard_data.json'):,} bytes)")
