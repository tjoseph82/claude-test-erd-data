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

    # ── DIAGNOSTIC: dump field names from ALL tables ──
    print("\n── DIAGNOSTICS ──")
    for tname, tdata in [
        ("Emergencies", emg), ("Classifications", cls), ("Offerings", eos),
        ("Reach_EO", reach), ("First_Service", fs_data), ("Offer_Perf", perf),
        ("Partners", partners_data), ("Indicators", indicators_data),
        ("Countries", countries),
    ]:
        print(f"  {tname}: {len(tdata)} records")
        if tdata:
            print(f"    Fields: {list(tdata[0].keys())}")

    # Build lookups
    country_map = {c["_id"]: c.get("Country", "") for c in countries}
    eo_map = {e["_id"]: e.get("Emergency Intervention Name", "") for e in eos}

    # Also build reverse map: EO name lookup by any linked record
    eo_name_map = {}
    for e in eos:
        name = e.get("Emergency Intervention Name", "")
        eo_name_map[e["_id"]] = name

    # Group classifications by Class ID (no FY filter here —
    # FY-filter at emergency level using "Earliest Classification").
    class_by_eid = defaultdict(list)
    # Also build a map from classification record ID → Class ID
    classrec_to_classid = {}
    for c in cls:
        cid = c.get("Class ID", "")
        if cid:
            class_by_eid[cid].append(c)
            classrec_to_classid[c["_id"]] = cid

    print(f"\n  Classifications grouped by Class ID: {len(class_by_eid)} unique IDs")

    # ── Build indexes for related tables ──
    # Index reach, first_service, offer_perf by classification record ID
    # AND by Class ID (human-readable) to handle both link types
    def build_index_by_classification(records, cls_field="Classification"):
        """Index records by classification record ID and by Class ID."""
        by_classrec = defaultdict(list)  # key: classification record ID
        by_classid = defaultdict(list)   # key: Class ID like "SS281"
        for r in records:
            link_val = r.get(cls_field, "")
            linked_ids = link_val if isinstance(link_val, list) else ([link_val] if link_val else [])
            for lid in linked_ids:
                by_classrec[lid].append(r)
                # Also map to Class ID if we know the mapping
                mapped_cid = classrec_to_classid.get(lid)
                if mapped_cid:
                    by_classid[mapped_cid].append(r)
        return by_classrec, by_classid

    reach_by_rec, reach_by_cid = build_index_by_classification(reach)
    fs_by_rec, fs_by_cid = build_index_by_classification(fs_data)
    perf_by_rec, perf_by_cid = build_index_by_classification(perf)
    partner_by_rec, partner_by_cid = build_index_by_classification(partners_data)
    ind_by_rec, ind_by_cid = build_index_by_classification(indicators_data)

    # Diagnostic: show how many records got indexed
    print(f"\n  Reach indexed: {len(reach_by_rec)} by record ID, {len(reach_by_cid)} by Class ID")
    print(f"  First_Service indexed: {len(fs_by_rec)} by record ID, {len(fs_by_cid)} by Class ID")
    print(f"  Offer_Perf indexed: {len(perf_by_rec)} by record ID, {len(perf_by_cid)} by Class ID")
    print(f"  Partners indexed: {len(partner_by_rec)} by record ID, {len(partner_by_cid)} by Class ID")
    print(f"  Indicators indexed: {len(ind_by_rec)} by record ID, {len(ind_by_cid)} by Class ID")

    # Show sample linking data
    if reach:
        sample = reach[0]
        print(f"\n  Sample reach record Classification field: {sample.get('Classification', 'MISSING')}")
        print(f"  Sample reach record all fields: {list(sample.keys())}")
    if fs_data:
        sample = fs_data[0]
        print(f"  Sample first_service Classification field: {sample.get('Classification', 'MISSING')}")
    if perf:
        sample = perf[0]
        print(f"  Sample offer_perf Classification field: {sample.get('Classification', 'MISSING')}")

    stance_rank = {"Red": 4, "Orange": 3, "Yellow": 2, "White": 1}

    # ── Build orange/red emergency records ──
    result = []
    debug_counts = {"no_eid": 0, "no_classifications": 0, "not_orange_red": 0, "wrong_fy": 0, "matched": 0}
    debug_program_data = {"reach_matched": 0, "fs_matched": 0, "perf_matched": 0}

    for e in emg:
        eid = e.get("Classification ID", "")
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

        # ── Collect related records using BOTH record-ID and Class-ID matching ──
        class_rec_ids = [c["_id"] for c in classifications]

        # Gather related reach records
        related_reach = []
        for crid in class_rec_ids:
            related_reach.extend(reach_by_rec.get(crid, []))
        if not related_reach:
            # Fallback: try matching by Class ID
            related_reach = reach_by_cid.get(eid, [])
        # Deduplicate by record ID
        seen_ids = set()
        unique_reach = []
        for r in related_reach:
            rid = r.get("_id", id(r))
            if rid not in seen_ids:
                seen_ids.add(rid)
                unique_reach.append(r)
        related_reach = unique_reach

        if related_reach:
            debug_program_data["reach_matched"] += 1

        # Gather related first_service records
        related_fs = []
        for crid in class_rec_ids:
            related_fs.extend(fs_by_rec.get(crid, []))
        if not related_fs:
            related_fs = fs_by_cid.get(eid, [])
        seen_ids = set()
        unique_fs = []
        for r in related_fs:
            rid = r.get("_id", id(r))
            if rid not in seen_ids:
                seen_ids.add(rid)
                unique_fs.append(r)
        related_fs = unique_fs

        if related_fs:
            debug_program_data["fs_matched"] += 1

        # Gather related offer_perf records
        related_perf = []
        for crid in class_rec_ids:
            related_perf.extend(perf_by_rec.get(crid, []))
        if not related_perf:
            related_perf = perf_by_cid.get(eid, [])
        seen_ids = set()
        unique_perf = []
        for r in related_perf:
            rid = r.get("_id", id(r))
            if rid not in seen_ids:
                seen_ids.add(rid)
                unique_perf.append(r)
        related_perf = unique_perf

        if related_perf:
            debug_program_data["perf_matched"] += 1

        # Gather related partner records
        related_partners = []
        for crid in class_rec_ids:
            related_partners.extend(partner_by_rec.get(crid, []))
        if not related_partners:
            related_partners = partner_by_cid.get(eid, [])

        # Gather related indicator records
        related_indicators = []
        for crid in class_rec_ids:
            related_indicators.extend(ind_by_rec.get(crid, []))
        if not related_indicators:
            related_indicators = ind_by_cid.get(eid, [])

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
            fs_list.append(
                {
                    "o": eo_map.get(eo_id, eo_id),
                    "d": fmt_date(f.get("Date of First Service")) or "—",
                }
            )

        # ── Offering review ──
        or_list = []
        for p in related_perf:
            eo_id = resolve_link(p.get("Emergency Offering", ""))
            or_list.append(
                {
                    "o": eo_map.get(eo_id, eo_id),
                    "q": p.get("Quality Assessment", ""),
                    "d": fmt_date(p.get("Date Assessed")) or "",
                }
            )

        # ── Partner data ──
        pd_list = []
        for p in related_partners:
            eo_id = resolve_link(p.get("Emergency Offering", "") or p.get("Offering", ""))
            pd_list.append({
                "partner": p.get("Partner Name", "") or p.get("Partner", "") or p.get("Name", ""),
                "offering": eo_map.get(eo_id, eo_id),
                "en": p.get("Existing or New", "") or p.get("Existing/New", ""),
                "disb": fmt_date(p.get("Date of First Disbursement") or p.get("First Disbursement")),
                "fs": fmt_date(p.get("Date of First Service") or p.get("First Service")),
                "fd": fmt_date(p.get("Funding Delivery") or p.get("Date of Funding Delivery")),
            })

        # ── Quality indicators ──
        qi_list = []
        for i in related_indicators:
            eo_id = resolve_link(i.get("Emergency Offering", "") or i.get("Offering", ""))
            qi_list.append({
                "o": eo_map.get(eo_id, eo_id),
                "v": i.get("Reported Value", "") or i.get("Value", ""),
                "t": i.get("Target", "") or i.get("Target Value", ""),
                "d": fmt_date(i.get("Date Entered") or i.get("Date")),
            })

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
    print(f"    With reach data: {debug_program_data['reach_matched']}")
    print(f"    With first_service data: {debug_program_data['fs_matched']}")
    print(f"    With offer_perf data: {debug_program_data['perf_matched']}")

    # Show sample data for debugging
    if debug_counts["matched"] > 0:
        sample_e = result[0] if result else None
        if sample_e:
            print(f"\n  Sample matched emergency '{sample_e['id']}':")
            print(f"    rbm entries: {len(sample_e['rbm'])}")
            print(f"    rbo entries: {len(sample_e['rbo'])}")
            print(f"    fs entries: {len(sample_e['fs'])}")
            print(f"    or entries: {len(sample_e['or'])}")
            print(f"    pd entries: {len(sample_e['pd'])}")
            print(f"    qi entries: {len(sample_e['qi'])}")

    if debug_counts["matched"] == 0 and emg:
        sample_eids = [e.get("Classification ID", "EMPTY") for e in emg[:5]]
        sample_class_ids = list(class_by_eid.keys())[:5]
        sample_dates = [(e.get("Classification ID",""), e.get("Earliest Classification","NO_FIELD"), e.get("Start Date","NO_FIELD")) for e in emg[:5]]
        print(f"    Sample emergency Classification IDs: {sample_eids}")
        print(f"    Sample class_by_eid keys: {sample_class_ids}")
        print(f"    Sample emergency dates (ID, Earliest Classification, Start Date): {sample_dates}")
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
    # Count from first_service data AND from reach data for completeness
    eo_counts = {}
    # From first service records
    for e in result:
        for f in e["fs"]:
            name = f["o"]
            eo_counts[name] = eo_counts.get(name, 0) + 1
    # If no fs data, fall back to counting unique EOs from reach data
    if not eo_counts:
        for e in result:
            seen_eos = set()
            for r in e["rbo"]:
                name = r["n"]
                if name and name not in seen_eos:
                    seen_eos.add(name)
                    eo_counts[name] = eo_counts.get(name, 0) + 1
    # If still empty, count from classification-level data
    if not eo_counts:
        for e in result:
            for o in e["or"]:
                name = o["o"]
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

    print(
        f"Done! dashboard_data.json written "
        f"({os.path.getsize('dashboard_data.json'):,} bytes)"
    )
