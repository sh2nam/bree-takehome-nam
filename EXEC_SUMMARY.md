# **Executive Summary — Case Study**

**Objective**  
Evaluate product changes and loan risk dynamics to optimize user growth, repayment, and profitability.  

**Key Insights**  
- **Experimentation (A/B Testing):**  
  - No significant **sample ratio mismatch (SRM)** or **temporal imbalance**, ensuring valid experiment setup.  
  - **Heterogeneity by risk decile**: persuasive prompt shows positive effects for **certain risk groups (deciles 3, 5, 7, 9)**, while social proof shows very small or insignificant lift.  

- **Funnel & Growth Analysis:**  
  - Overall open→disbursed conversion: **47.5%**.  
  - Largest drop-offs: open→link (-28.1pp), request→approved (-23.9pp), and link→request (-13.2pp).  
  - Channel-level analysis (e.g., **Ontario organic**) shows outsized impact on disbursement growth.  
  - **Recommendations:**  
    - Improve **bank linking reliability** to improve open→link conversion.  
    - Allow **multiple bank connections** to access more transaction detail and improve link→request + request→approved conversion.  
    - Find **optimal way to present tip and instant transfer fee** to improve link→request conversion.  
    - Review **approval criteria** to improve request→approved conversion.  
    - Conduct **survey of Ontario organic users** to diagnose drop-offs after linking.  

- **Risk Modeling (ML):**  
  - Baseline risk score is strong; additional risk features such as **cashflow dynamics, delinquency/repayment history, exposure measures, and borrower behavioral patterns** provided marginal uplift, suggesting more exploration required with the risk features created.  
  - **Logistic regression with lasso regularization** tested; **AUC ~0.58**, only slightly above random.  
  - Profitability constraints unmet with current feature set, suggesting:  
    - More risk features and models with stronger predictive power need to be explored (not completed due to time constraints but would be a priority going forward).  
    - Further optimization of **loan size assignment** and **instant transfer fee assignment**.  

- **Data Engineering:**  
  - Built ETL pipeline to integrate loan, repayment, and mobility datasets into DuckDB for fast iteration.  
  - Automated feature generation (rolling exposure, delinquency counts, repayment lags) across loan history.  

**Recommendations**  
1. **Experimentation:** Persuasive prompt — heterogeneity exists by risk decile; explore targeted messaging by risk score. Social proof — lift is very small/insignificant; re-engineer the whole approach before retesting.  
2. **Growth Funnel:** Focus on bank linking reliability, multi-bank connection, tip/fee presentation, and approval criteria to unlock conversion gains.  
3. **Risk Modeling:** Expand features and modeling approaches; optimize loan size and instant transfer fees.  
