# Part A — Design & Guardrails

### Objective  
Test an updated fee structure (**PriceTest_B**) vs current (**PriceTest_A**) to improve **Revenue per Disbursed Loan (RPDL)** without harming credit risk or funnel conversion.  

### Hypotheses  
- **Primary:**  
  - **H1:** PriceTest_B increases **RPDL** vs PriceTest_A.  
- **Guardrails:**  
  - **H2:** Default rate not worse than +0.2 pp.  
  - **H3:** Approval rate not worse than −1.0 pp.  
  - **H4:** No SRM imbalance (χ² p > 0.05).  
- **Secondary:**  
  We can also test **PnL per Loan** (revenue minus write-offs) since it aligns more closely to profitability than revenue alone. Because tip prompts are running at the same time, they can also affect revenue and PnL — so any analysis must control for both Price and Tip variants to avoid confounding.  

### Power Analysis (from baseline data)  
**Baseline (PriceTest_A, disbursed loans):**  
- Mean RPDL = **$0.32**  
- Assume σ = **$0.32** (σ ≈ µ rule of thumb)  
- Total disbursed loans = **1,756** over **138 days**  
- ≈ **13 disbursed loans/day in Price A**  
- α = 0.05, Power = 80%  

| MDE (relative lift) | Abs Δ ($) | n per arm | Horizon (@ ~13/arm/day) |
|---------------------|-----------|-----------|--------------------------|
| **10%** | ~$0.03 | ~1,568 | ~121 days (~4 months) |
| **15%** | ~$0.05 | ~697  | ~54 days (~2 months)  |
| **20%** | ~$0.06 | ~392  | ~31 days (~1 month)   |

**Plan:** Target **10–15% MDE** as realistic. At current volumes, expect **2–4 months** to reach power. Covariate adjustment (province, risk decile, device_os, loan amount) can reduce variance and shorten horizon.  

### Summary of Plan  
- Randomize eligible users 50/50 into **A** vs **B**.  
- Track **RPDL** as primary outcome.  
- Guardrails (default, approval, SRM) ensure credit risk and funnel health are not harmed.  
- Secondary hypothesis on **PnL per loan** provides a risk-adjusted profitability view, while carefully accounting for overlapping TipPrompt effects.  
- **Target:** Detect a **10–15% lift in RPDL (~$0.03–$0.05)** with ~**700–1,600 users per arm** over ~**2–4 months** at current volumes.  
- Decision: Roll out B if **RPDL lift is positive** and **all guardrails hold**.  


# Part B — Design & Guardrails


### Naïve Mean Differences (unadjusted)  
- **Persuasive prompt**:  
  - Tip take-rate **+4.1 pp** vs control.  
  - Mean tip (among tippers) **+$0.26**.  
  - RPDL **+20% (+$0.34)**.  
  - Default rate unchanged (+0.17 pp).  
- **Social proof prompt**:  
  - Small lift in take-rate (+0.8 pp) and mean tip (+$0.10).  
  - No gain in RPDL (-1.1%, –$0.02).  
  - Default rate flat (+0.3 pp).  

👉 Naïvely, persuasive looks like a clear winner on tips and revenue; social proof is flat.  

---

### Adjusted Results (regression controlling for price variant, risk decile, province, and loan amount)  
- **Persuasive prompt**:  
  - Take-rate **+3.8 pp** (CI [1.0, 6.5], p=0.007).  
  - Mean tip (incl. zeros) **+$0.14** (CI [$0.04, $0.23], p=0.004).  
  - RPDL **+$0.10**, positive but not statistically significant.  
  - Default rate unchanged (~+0.4 pp, not significant).  
- **Social proof prompt**:  
  - No significant effects on take-rate (+1.0 pp, ns), mean tip (+$0.04, ns), RPDL (–$0.02, ns), or default.  

👉 After adjustment, the **persuasive prompt remains the clear winner**, driving a significant lift in both tip adoption and tip amounts with no harm to default. **Social proof shows no meaningful effect.** 

#### SRM (Sample Ratio Mismatch)  
- Control: **34.5%**, Persuasive: **32.3%**, Social Proof: **33.2%**  
- **Chi² = 3.82, p = 0.15** → no evidence of SRM.  
- ✅ Assignments are balanced across variants, consistent with randomization.  

---

#### Temporal Imbalance  
- **Weekday:** Variant shares range ~31–37% across Mon–Sun.  
  - **Chi² = 15.97, p = 0.19** → no evidence of imbalance.  
- **Month:** Shares are stable across Mar–Jul (most ~31–37%).  
  - **Chi² = 10.45, p = 0.24** → no evidence of imbalance.  
- ✅ No weekday or month effects detected; assignment proportions are consistent over time.  

---

#### Heterogeneity by Risk Decile  
- **Persuasive prompt:**  
  - Significant positive effects in **deciles 3, 5, 7, 9** (+9–10 pp take-rate lift, p<0.05).  
  - No significant lift in deciles 1–2 (flat/negative) or very high deciles (8, 10).  
- **Social proof prompt:**  
  - No reliable positive effects; mostly flat.  
  - **Negative significant effect in decile 6 (–10.3 pp, p=0.024)** → possible backfire.  
- ⚠️ Treatment effects are heterogeneous: persuasive works best in **mid/high risk deciles**, while social proof is ineffective and sometimes harmful.  

---

**Overall takeaway:**  
- ✅ Randomization was valid (**no SRM**).  
- ✅ Assignments were stable across time (**no temporal imbalance**).  
- ⚠️ Treatment effects are **heterogeneous by risk decile**: persuasive prompt is effective in mid/high risk groups, social proof is not.


# Part C — Recommendation

The **persuasive tip prompt** is the clear winner: it significantly increased tip take-rate (+3.8 pp) and average tip size (+$0.14) with no harm to default rates. The **social proof prompt** showed no consistent benefit and even reduced tipping in some risk deciles (e.g., –10 pp in decile 6).

Because treatment effects are **heterogeneous by risk decile**, I would **not roll out globally**. Instead, I recommend **targeting persuasive prompts to mid/high risk users (deciles 3, 5, 7, 9)** where the effect is strongest, while holding back for very low-risk or very high-risk segments where the lift is unclear.  

**Follow-up test:** run a targeted experiment applying persuasive prompts only to those deciles, validating lift in tips and ensuring no hidden risk trade-offs.
