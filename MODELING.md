# Risk Modeling Case Study – Summary Write-Up

## Objective
The case study objective was to **maximize expected profit** subject to:
- **Default rate ≤ 15% among approved loans**
- **Approval rate ≥ 30% overall**

The task required building a predictive risk model, evaluating performance, and designing a decision policy that maps predicted risk to **approval, loan limits, and instant transfer eligibility**.

---

## Risk Features Used in the Model

### **1. User & Acquisition**
- `device_os`  
- `province`  
- `acquisition_channel`  
- `signup_month`  
- `payroll_frequency`  
- `fico_band`  
- `baseline_risk_score`  
- `risk_score_decile`  

---

### **2. Income Cadence & Stability**
- `median_payroll_gap_days`  
- `payroll_frequency_bucket`  
- `days_since_last_payroll`  
- `inflow_cv_30d`  
- `total_inflow_30d`  

**Why useful:**  
Stable and predictable income patterns (e.g., consistent payroll frequency, low inflow volatility) suggest better repayment ability. Long gaps or high variability in income increase risk.  

---

### **3. Liquidity & Balances**
- `avg_eod_balance_14d`, `avg_eod_balance_30d`  
- `min_balance_14d`  
- `overdraft_days_14d`, `overdraft_days_30d`  

**Why useful:**  
Low or negative balances and frequent overdrafts are **direct signals of liquidity stress**. Higher average balances provide a cushion, lowering default risk.  

---

### **4. Cashflows (Levels & Ratios)**
- `inflow_sum_14d`, `outflow_sum_14d`, `total_outflow_30d`  
- `outflow_to_inflow_14d`, `outflow_to_inflow_30d`  

**Why useful:**  
Measure whether **spending systematically exceeds income**. High outflow-to-inflow ratios point to structural overspending, a key predictor of delinquency.  

---

### **5. Expense Pressure / Budget Shares**
- `rent_share_30d`  
- `essentials_share_14d`, `essentials_share_30d`  
- `discretionary_share_14d`, `discretionary_share_30d`  

**Why useful:**  
Budget allocation signals **financial stress**:  
- Rent-heavy households may be overburdened.  
- High essentials share = less discretionary room = tighter budgets.  
- Very high discretionary share = volatile or less disciplined spending.  

---

### **6. Momentum & Volatility**
- `net_cf_momentum_14d`, `net_cf_momentum_30d`  
- `inflow_volatility_14d`, `inflow_volatility_30d`  

**Why useful:**  
- **Momentum** = whether net cash flow is trending positive or negative. Declining momentum is a red flag.  
- **Volatility** = unstable inflows or spending patterns often correlate with higher risk.  

---

### **7. Balance Cushion Ratios**
- `balance_to_outflow_14d`, `balance_to_outflow_30d`  
- `balance_to_inflow_14d`, `balance_to_inflow_30d`  

**Why useful:**  
Scaling balances to inflows/outflows provides a **relative measure of liquidity**. Even small balances may be adequate if outflows are low; large balances relative to inflows/outflows signal strong repayment capacity.  

---

### **8. Historical Loan Performance**
- `hist_repay_rate`  
- `hist_default_rate`  
- `hist_default_count`  
- `hist_avg_late_days`  
- `hist_avg_late_days_count`  

**Why useful:**  
Past behavior is often the **best predictor of future behavior**. Prior defaults or chronic lateness are highly predictive of risk.  
⚠️ *Note*: I made sure to look at previous loans rather than the entire loan history to avoid label leakage.

---

👉 **In summary:**  
The feature set spans **structural risk (user/acquisition), income stability, liquidity, spending pressure, cashflow dynamics, and history**. In practice, however, most of the incremental features added little beyond Bree’s **baseline risk score**, which already encodes much of the same information.  


> **Observation:** In the final model, **`baseline_risk_score` and `risk_score_decile` dominated**. Most engineered features did not contribute significantly, indicating that Bree’s existing risk score already captures most of the predictive signal.

---

## Modeling Approach
- **Model:** LASSO Logistic Regression (`penalty="l1"`)  
- **Regularization:** Tuned via cross-validation across multiple `C` values  
- **Data:** Restricted to loans that were **approved (disbursed or defaulted)** to avoid leakage from unlabeled cases  
- **Handling missing values:** Filled with medians  

---

## Model Performance
- **AUC/ROC:** ~0.58  
- **Average Precision (AP):** ~0.31  
- **Calibration:** Predictions poorly calibrated; observed vs predicted default rates diverge  
- **Interpretation:** Performance is weak. The model effectively collapses to Bree’s baseline risk score, with little incremental predictive value from engineered features.

---

## Confusion at Operating Points
Two decision policies were tested:

1. **Fixed Approval Rate (e.g., 35%)**  
   - Threshold chosen so that 35% of applicants are approved.  
   - Result: Approval ~35%, default among approved ~19–20%.  

2. **Fixed Default Target (≤19%)**  
   - Threshold chosen to minimize defaults among approved.  
   - Could not achieve **≤15% default rate** while keeping approval >20%.  
   - Shows the engineered features are not sufficiently discriminative.

---

## Unit Economics Simulation
For both policies, we calculated expected PnL using:
- **Revenue per loan = fee + tip + instant transfer fee**  
- **Expected loss = PD × loan amount × LGD (0.85)**  
- **Processing cost = 0.3 + 0.012 × loan amount**  
- **Capital cost = 12% APR prorated by days outstanding**

**Loan Limits & Transfer Policy:**
- Approved users assigned loan amounts based on PD quartiles:  
  - Lowest risk: $200  
  - Medium risk: $150 / $100  
  - Highest risk: $75  
- Instant transfer: Eligible for all users  

**Results:**
- Both policies produced **negative PnL**, even with conservative limits.  
- To make portfolio PnL positive, loan sizes had to be set unrealistically low.  
- Confirms the model is not strong enough to support profitable decisioning.

---

## Key Takeaways

- I relied heavily on AI to generate and include as many risk features as possible.  
  With limited time, I may have overlooked important candidate features, or not handled **data accuracy / missing values** as rigorously as I should have.  

- **Baseline risk score** already appears to be a powerful predictor.  
  My engineered features added little incremental value — the regularized logistic regression model consistently shrank their coefficients to zero.  

- **Model performance was weak**:  
  - AUC ~ **0.58**, only slightly better than random guessing (0.50).  
  - Precision–recall and calibration curves confirmed that predictive power was limited.  

- **Profitability constraint was not met**:  
  Despite experimenting with decision policies (fixed approval rates and fixed default targets), I was not able to achieve both  
  - **Default rate ≤ 15% among approved** and  
  - **Approval rate ≥ 30%**.  

- **Unit economics were negative** under the tested policies.  
  I had to assign artificially small loan amounts to generate positive profit, which is not realistic in practice.  

---

### Next Steps (if I had more time)
1. Re-examine the **quality and construction** of engineered features.  
   - Ensure accurate definitions (e.g., inflow/outflow ratios).  
   - Handle missing values more thoughtfully (e.g., segment-wise imputation, median fills).  
2. Explore **additional predictive signals**:  
   - Longer transaction history (beyond 30 days).  
   - Behavioral features (engagement, repayment channel).  
   - Interaction terms between liquidity and spending pressure.  
3. Test alternative models (e.g., gradient boosting, ensemble methods) alongside logistic regression.  
4. Consider **separate models** for:
   - Approval (PD estimation)  
   - Loan size allocation (limit assignment)  
   - Pricing adjustments (fees / instant transfer eligibility).  
