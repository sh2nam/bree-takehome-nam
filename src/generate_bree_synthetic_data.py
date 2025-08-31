import os, json, math, random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

random.seed(42); np.random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUT_DIR, exist_ok=True)

start_date = datetime(2025, 1, 1)
end_date   = datetime(2025, 7, 31)

PROVINCES = ["ON","QC","BC","AB","MB","SK","NS","NB","NL","PE"]
PROV_W    = [0.38,0.22,0.14,0.12,0.04,0.03,0.025,0.02,0.015,0.01]
CHANNELS  = ["organic","paid_search","paid_social","referral","affiliate","email"]
DEVICES   = ["ios","android"]
PAYFREQ   = ["weekly","biweekly","semimonthly","monthly","unknown"]

def rand_date(a=start_date, b=end_date):
    return a + timedelta(days=random.randint(0, (b-a).days))

def sample_weighted(items, weights):
    return np.random.choice(items, p=np.array(weights)/np.sum(weights))

N_USERS = 10000
users = []
for uid in range(1, N_USERS+1):
    signup_at = rand_date()
    province  = sample_weighted(PROVINCES, PROV_W)
    device    = random.choice(DEVICES)
    channel   = np.random.choice(CHANNELS, p=[0.45,0.12,0.18,0.10,0.10,0.05])
    linked    = np.random.rand() < 0.72
    bank_linked_at = signup_at + timedelta(hours=np.random.exponential(30)) if linked else None
    pfreq = np.random.choice(PAYFREQ, p=[0.25,0.45,0.15,0.10,0.05])
    # baseline risk (0=best,1=worst), correlated with channel and province a bit
    base_risk = np.clip(np.random.beta(2,5) + (0.03 if channel in ["paid_social","affiliate"] else 0)
                        + (0.02 if province in ["NB","NL","PE"] else 0), 0, 1)
    # FICO-like band for color only
    if  base_risk < 0.15: fico = "740+"
    elif base_risk < 0.25: fico = "680-739"
    elif base_risk < 0.40: fico = "640-679"
    elif base_risk < 0.60: fico = "580-639"
    elif base_risk < 0.80: fico = "500-579"
    else:                   fico = "<500"

    users.append({
        "user_id": uid,
        "signup_at": signup_at.isoformat(),
        "province": province,
        "device_os": device,
        "acquisition_channel": channel,
        "bank_linked_at": bank_linked_at.isoformat() if bank_linked_at else "",
        "payroll_frequency": pfreq,
        "baseline_risk_score": round(base_risk, 4),
        "fico_band": fico
    })
users_df = pd.DataFrame(users)
users_df.to_csv(os.path.join(OUT_DIR,"users.csv"), index=False)

# Experiments: overlapping and assigned at first session
EXPERIMENTS = [
    # Pricing structure test impacts fee components
    {"name":"PriceTest_2025Q2","start":"2025-03-15","end":"2025-06-30","variants":["A","B"]},
    # Tip prompt copy test impacts tip uptake
    {"name":"TipPrompt_2025Q2","start":"2025-04-01","end":"2025-07-15","variants":["control","persuasive","social_proof"]},
]
assignments = []
for _, row in users_df.iterrows():
    first_seen = datetime.fromisoformat(row["signup_at"])
    for exp in EXPERIMENTS:
        s = datetime.fromisoformat(exp["start"])
        e = datetime.fromisoformat(exp["end"])
        # assigned if user active in window
        if s <= first_seen <= e:
            variant = np.random.choice(exp["variants"])
            assignments.append({
                "assignment_id": f"{row.user_id}-{exp['name']}",
                "user_id": row.user_id,
                "experiment_name": exp["name"],
                "variant": variant,
                "assigned_at": first_seen.isoformat()
            })
pd.DataFrame(assignments).to_csv(os.path.join(OUT_DIR, "ab_assignments.csv"), index=False)

# Sessions: minimal funnel events
EVENTS = [
    ("app_open", 1.0),
    ("view_onboarding", 0.95),
    ("link_bank_start", 0.80),
    ("link_bank_success", 0.72),
    ("start_advance_request", 0.60),
    ("submit_advance_request", 0.52),
    ("approved", 0.40),
    ("disbursed", 0.39)
]

sessions = []
for _, u in users_df.iterrows():
    t = datetime.fromisoformat(u["signup_at"])
    progressed = True
    for ev, p in EVENTS:
        if progressed and np.random.rand() < p:
            sessions.append({
                "event_id": f"{u.user_id}-{ev}-{random.randint(1,10**9)}",
                "user_id": u["user_id"],
                "session_id": f"sess-{u['user_id']}",
                "ts": (t + timedelta(minutes=random.randint(0, 120))).isoformat(),
                "event_name": ev,
                "screen": "onboarding" if "bank" in ev else ("loan" if "request" in ev or "disbursed" in ev else "home"),
                "properties_json": json.dumps({})
            })
        else:
            progressed = False
sessions_df = pd.DataFrame(sessions)
sessions_df.to_csv(os.path.join(OUT_DIR,"sessions.csv"), index=False)

# Transactions: payroll + expenses; balances; basic features
def gen_transactions_for_user(u):
    txns = []
    start = datetime(2025,1,1)
    end   = datetime(2025,7,31)
    day = start
    balance = max(0, np.random.normal(200, 150))
    paygap = {"weekly":7, "biweekly":14, "semimonthly":15, "monthly":30, "unknown":14}[u["payroll_frequency"]]
    next_pay = datetime.fromisoformat(u["signup_at"]) + timedelta(days=random.randint(0, paygap))
    while day <= end:
        # payroll?
        if day >= next_pay:
            payroll_amt = max(400, np.random.normal(950, 220))
            balance += payroll_amt
            txns.append({
                "txn_id": f"t-{u['user_id']}-{int(day.timestamp())}-in",
                "user_id": u["user_id"],
                "posted_date": day.date().isoformat(),
                "amount": round(payroll_amt,2),
                "direction": "inflow",
                "mcc": "6011",
                "category": "payroll",
                "balance_after": round(balance,2),
                "is_payroll": 1
            })
            next_pay += timedelta(days=paygap)
        # expenses (Poisson)
        n_out = np.random.poisson(1.2)
        for _ in range(n_out):
            amt = max(5, np.random.lognormal(mean=3.2, sigma=0.7))
            balance -= amt
            cat = np.random.choice(
                ["rent","groceries","dining","utilities","transport","entertainment","other"],
                p=[0.08,0.32,0.18,0.10,0.18,0.06,0.08]
            )
            txns.append({
                "txn_id": f"t-{u['user_id']}-{int(day.timestamp())}-out-{random.randint(1,1e6)}",
                "user_id": u["user_id"],
                "posted_date": day.date().isoformat(),
                "amount": round(-amt,2),
                "direction": "outflow",
                "mcc": "0000",
                "category": cat,
                "balance_after": round(balance,2),
                "is_payroll": 0
            })
        day += timedelta(days=1)
    return txns

txn_rows = []
# Sample a subset to keep size manageable
sample_users = users_df.sample(frac=0.35, random_state=42)  # ~3500 users
for _, u in sample_users.iterrows():
    txn_rows.extend(gen_transactions_for_user(u))
pd.DataFrame(txn_rows).to_csv(os.path.join(OUT_DIR,"transactions.csv"), index=False)

# Loans: request->approve->disburse->repay/default; pricing linked to experiments
def loan_rows_for_user(u):
    rows = []
    signup = datetime.fromisoformat(u["signup_at"])
    n_loans = np.random.poisson(1.2)
    if n_loans == 0: return rows
    # experiment variants
    assign = [a for a in assignments if a["user_id"]==u["user_id"]]
    v_price = next((a["variant"] for a in assign if a["experiment_name"]=="PriceTest_2025Q2"), "A")
    v_tip   = next((a["variant"] for a in assign if a["experiment_name"]=="TipPrompt_2025Q2"), "control")
    for i in range(n_loans):
        requested_at = signup + timedelta(days=int(np.random.exponential(25))+i*max(7, int(np.random.exponential(18))))
        if not (start_date <= requested_at <= end_date): continue
        amount = float(np.clip(np.random.normal(140, 60), 40, 350))
        base_approve_p = 0.82 - 0.6*u["baseline_risk_score"] + 0.03*(u["device_os"]=="ios")
        approved = np.random.rand() < max(0.05, min(0.95, base_approve_p))
        approved_at = requested_at + timedelta(hours=np.random.exponential(12)) if approved else ""
        disbursed_at = approved_at + timedelta(hours=np.random.exponential(6)) if approved else ""
        due_date = (disbursed_at + timedelta(days=14)).date().isoformat() if approved else ""

        # Pricing by variant
        if v_price=="A":
            fee = 0.0
            instant_fee = 0.0
        else:
            fee = round(np.clip(0.01*amount + np.random.normal(0.4,0.2), 0, 6.0), 2)
            instant_fee = round(np.random.choice([0.0, 1.99, 2.99], p=[0.45,0.35,0.20]), 2)

        # Tip probability affected by tip prompt
        tip_uplift = {"control":0.00, "persuasive":0.06, "social_proof":0.03}[v_tip]
        tip = 0.0
        if approved:
            tip_prob = np.clip(0.12 + tip_uplift - 0.10*u["baseline_risk_score"], 0.01, 0.4)
            if np.random.rand() < tip_prob:
                tip = round(np.random.choice([1,2,3,5,7], p=[0.25,0.30,0.25,0.15,0.05]),2)

        # Default probability conditioned on risk + amount + instant opt‑in (proxy for liquidity stress)
        instant_opt_in = approved and (instant_fee>0) and (np.random.rand() < (0.32 - 0.18*u["baseline_risk_score"]))
        default_p = 0.06 + 0.45*u["baseline_risk_score"] + 0.0005*amount + (0.02 if instant_opt_in else 0)
        default_p = np.clip(default_p, 0.01, 0.45)
        defaulted = approved and (np.random.rand() < default_p)
        repaid_at = ""
        late_days = 0
        status = "requested"
        principal_repaid = 0.0
        writeoff = 0.0
        if approved:
            status = "approved"
            if disbursed_at:
                status = "disbursed"
                if not defaulted:
                    late_days = max(0, int(np.random.normal(1.4, 2.0)))
                    repay_ts = disbursed_at + timedelta(days=14+late_days)
                    repaid_at = repay_ts.isoformat()
                    status = "repaid"
                    principal_repaid = amount
                else:
                    late_days = int(np.random.normal(20, 7))
                    status = "default"
                    writeoff = round(amount * np.random.uniform(0.6, 0.95), 2)

        rows.append({
            "loan_id": f"L{u['user_id']}-{i+1}",
            "user_id": u["user_id"],
            "requested_at": requested_at.isoformat(),
            "approved_at": approved_at if approved_at else "",
            "disbursed_at": disbursed_at if disbursed_at else "",
            "due_date": due_date,
            "repaid_at": repaid_at,
            "amount": round(amount,2),
            "fee": round(fee,2),
            "tip_amount": round(tip,2),
            "instant_transfer_fee": round(instant_fee,2),
            "status": status,
            "late_days": late_days,
            "chargeoff_flag": 1 if status=="default" else 0,
            "autopay_enrolled": 1 if np.random.rand()<0.62 else 0,
            "principal_repaid": round(principal_repaid,2),
            "writeoff_amount": writeoff,
            "price_variant": v_price,
            "tip_variant": v_tip
        })
    return rows

loan_rows = []
for _, u in users_df.iterrows():
    loan_rows.extend(loan_rows_for_user(u))
pd.DataFrame(loan_rows).to_csv(os.path.join(OUT_DIR,"loans.csv"), index=False)

print("Synthetic data written to ./data")