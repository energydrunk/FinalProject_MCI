# DustiniaDelixia Groceria : Finance & Payment Analyst Pipeline
**Final Project Lab MCI 2026 | Persona: Finance Analyst**

Embun Nabila Rasendriya Az Zahra - 5025241009
---

## Project Structure

```
finalproject-mci/
├── docker-compose.yml          
├── Dockerfile                  
├── requirements.txt            
├── data/                       
│   ├── orders.csv
│   ├── order_payments.csv
│   ├── customers.csv
│   ├── order_items.csv
│   ├── sellers.csv
│   ├── products.csv
│   └── category_translation.csv
└── dags/
    ├── finance_pipeline.py
    └── scripts/
        ├── extract_and_validate.py   
        ├── transform_payments.py     
        └── load_to_clickhouse.py     
```

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Apache Airflow | 2.9.1 | Pipeline orchestration |
| Apache Spark (PySpark) | 3.5.1 | Data transformation |
| ClickHouse | 26.3.9 | Analytical database |
| Metabase | Latest | BI dashboard |
| PostgreSQL | 13 | Airflow metadata |
| Docker | - | Containerization |

---

## How to Run

### 1. Prerequisites
- Docker & Docker Compose installed
- Extract dataset ZIP to `data/` folder

### 2. Start all services
```bash
docker compose up --build -d
docker compose run --rm airflow-init
docker compose up airflow-webserver -d
```

### 3. Trigger pipeline
Open http://localhost:8080 → login `admin/admin` → trigger DAG `finance_payment_pipeline`



### 4. Verify ClickHouse
```bash
docker exec -it finalproject-mci-clickhouse-server-1 clickhouse-client --user admin --password rahasia
```
```sql
USE analytics;
SHOW TABLES;
```

### 5. Setup Metabase
Open http://localhost:3000 → Add database:
- **Host:** `clickhouse-server`
- **Port:** `8123`
- **Username:** `admin`
- **Password:** `rahasia`

---

## Start / Stop Docker

```bash
# Stop
docker compose down

# Start again (no need to re-init)
docker compose up -d
docker compose up airflow-webserver -d
```

---

## Pipeline Architecture

```
Raw CSV (data/)
    │
    Task 1: extract_and_validate.py
    │  - Read all CSVs (orders, payments, customers, items, sellers, products, categories)
    │  - Keep ALL order statuses (delivered, canceled, unavailable)
    │  - Remove payment_type = 'not_defined'
    │  - Save as Parquet to data_lake/
    │
    Task 2: transform_payments.py (Apache Spark)
    │  - Build fact_payments (all orders, all statuses)
    │  - Segment customers by spending threshold (Regular/High/Premium)
    │  - Aggregate revenue by state (delivered only)
    │  - Analyze installments vs order value
    │  - Identify revenue leakage (canceled/unavailable)
    │  - Enrich order items with product categories
    │  - Calculate seller cancel rates
    │  - Save as Parquet to data_lake/transformed/
    │
    Task 3: load_to_clickhouse.py
       - Create tables if not exists
       - Truncate and reload all 8 tables
       - Load to ClickHouse database: analytics
```

---

## ClickHouse Tables (database: analytics)

| Table | Rows | Description |
|---|---|---|
| `fact_payments` | 99,437 | One row per order all statuses, total payment, payment type, installments |
| `customer_segments` | 96,477 | Customer segmentation based on delivered spending |
| `revenue_by_state` | 27 | Aggregated revenue per state (delivered only) |
| `installment_analysis` | 24 | Avg order value per installment count |
| `payment_method_analysis` | 5 | Revenue & orders breakdown per payment method |
| `revenue_leakage` | 1,231 | Canceled & unavailable orders |
| `order_items_products` | 112,650 | Order items enriched with product category |
| `seller_orders` | 3,095 | Cancel rate per seller |

### Table Schemas

**fact_payments**
```sql
order_id String, customer_id String, order_status String,
order_purchase_timestamp String, total_payment Float64,
max_installments Int32, payment_type String,
customer_unique_id String, customer_state String
```

**customer_segments**
```sql
customer_id String, customer_unique_id String, customer_state String,
total_spent Float64, order_count Int32, segment String
```

**revenue_leakage**
```sql
order_id String, order_status String, payment_type String,
customer_state String, total_payment Float64,
leakage_type String, order_purchase_timestamp String
```

**order_items_products**
```sql
order_id String, product_id String, seller_id String,
price Float64, freight_value Float64, category String
```

**seller_orders**
```sql
seller_id String, seller_state String, total_orders Int64,
canceled_orders Int64, cancel_rate_pct Float64
```

---

## Customer Segmentation Logic

Segmentation is based on **total spending per customer** from delivered orders only, using fixed thresholds derived from actual data distribution analysis:

```python
cust_segments = cust_spend.withColumn(
    "segment",
    F.when(F.col("total_spent") >= 400, "Premium")
    .when(F.col("total_spent") >= 200, "High")
    .otherwise("Regular")
)
```

| Segment | Threshold | % of Customers | % of Revenue |
|---|---|---|---|
| Regular | < R$200 | 79.74% | 46.5% |
| High | R$200 – R$400 | 14.21% | 24.0% |
| Premium | ≥ R$400 | 6.04% | 29.5% |

**Justification for thresholds:**
- **R$200** → natural break in distribution: customer count drops from 48K → 18K
- **R$400** → cumulative percentile reaches ~95%, representing top 5% customers
- "Very High" tier removed : no significant natural break between R$200–R$400

---

## Key Insights

- **Total delivered orders:** 96,477 | **Total revenue:** R$15.4M
- **SP dominates** revenue at R$2.9M HV revenue, followed by RJ and MG
- **Credit card** is used in 79.63% of all transactions
- **Installments drive higher order values** — 2x+ installment orders avg R$185 vs R$120 for single payment
- **Premium segment (6% of customers) contributes 29.5% of total revenue**
- **Revenue leakage: R$269,735** from 1,231 canceled/unavailable orders
- **Voucher repeat rate (7.08%) > Non-voucher (2.94%)** but avg order value is lower with voucher
- **1,701 near-threshold customers** using CC 1x = highest priority for installment upsell

---

## Metabase Dashboard

Dashboard organized into 5 sections:

1. **Executive Summary** : spending distribution, order overview, segment composition
2. **HV Customer Behaviour** : revenue trend, quarterly breakdown, repeat purchase, transaction timing
3. **Payment Behaviour & Preferences** : payment method by segment, installment analysis, voucher effectiveness
4. **Geographic Distribution** : HV revenue by state, concentration, opportunity matrix
5. **Revenue Leakage & Recommendations** : canceled orders, cancel rate, upgrade scenarios

---

## SQL Queries : All Dashboard Questions

**Conventions:**
- HV = High Value = `total_spent >= 200` (High + Premium segments)
- Near-threshold = `total_spent BETWEEN 150 AND 200`
- All queries use prefix `analytics.table_name`

---

### Executive Summary

**Q0a — Spending Distribution (Line Chart)**
```sql
SELECT
    floor(total_spent / 100) * 100 AS spending_bin,
    count(*) AS customer_count
FROM analytics.customer_segments
GROUP BY spending_bin
ORDER BY spending_bin
LIMIT 20
```

**Q0b — Customer Spending Distribution Table**
```sql
SELECT
    floor(total_spent / 100) * 100 AS spending_bin,
    count(*) AS customer_count,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS pct_customers,
    round(sum(count(*)) OVER (ORDER BY floor(total_spent/100)*100) * 100.0 / sum(count(*)) OVER (), 2) AS cumulative_pct
FROM analytics.customer_segments
GROUP BY spending_bin
ORDER BY spending_bin
```

**Q19 — Order Value Distribution (Histogram)**
```sql
SELECT
    CASE
        WHEN total_payment < 50   THEN 'R$0-50'
        WHEN total_payment < 100  THEN 'R$50-100'
        WHEN total_payment < 150  THEN 'R$100-150'
        WHEN total_payment < 200  THEN 'R$150-200'
        WHEN total_payment < 250  THEN 'R$200-250'
        WHEN total_payment < 300  THEN 'R$250-300'
        WHEN total_payment < 400  THEN 'R$300-400'
        WHEN total_payment < 500  THEN 'R$400-500'
        ELSE 'R$500+'
    END AS value_bin,
    count(*) AS total_orders
FROM analytics.fact_payments
WHERE order_status = 'delivered'
GROUP BY value_bin
ORDER BY min(total_payment)
```

**Q12 — Total Orders Summary Card**
```sql
SELECT count(*) AS total_orders
FROM analytics.fact_payments
WHERE order_status = 'delivered'
```

**Q11 — Actual Revenue vs Target**
```sql
SELECT
    round(sum(total_payment), 2) AS actual_revenue,
    round(sum(total_payment) * 1.2, 2) AS target_revenue
FROM analytics.fact_payments
WHERE order_status = 'delivered'
```

**Q1 — Order Composition by Segment (Donut)**
```sql
SELECT segment, count(*) AS total_orders
FROM analytics.customer_segments
GROUP BY segment
ORDER BY total_orders DESC
```

**Q2 — Revenue Contribution by Segment (Donut)**
```sql
SELECT
    cs.segment,
    round(sum(fp.total_payment), 2) AS total_revenue
FROM analytics.customer_segments cs
JOIN analytics.fact_payments fp ON cs.customer_id = fp.customer_id
WHERE fp.order_status = 'delivered'
GROUP BY cs.segment
ORDER BY total_revenue DESC
```

**Q10 — Monthly HV Revenue Trend**
```sql
SELECT
    formatDateTime(toDateTime(fp.order_purchase_timestamp), '%Y-%m') AS month,
    round(sum(fp.total_payment), 2) AS hv_revenue
FROM analytics.fact_payments fp
JOIN analytics.customer_segments cs ON cs.customer_id = fp.customer_id
WHERE cs.total_spent >= 200
AND fp.order_status = 'delivered'
AND fp.order_purchase_timestamp != ''
GROUP BY month
ORDER BY month
```

---

### HV Customer Behaviour

**Q13 — HV Revenue by Quarter**
```sql
SELECT
    concat('Q', toString(toQuarter(toDateTime(fp.order_purchase_timestamp))),
           '-', toString(toYear(toDateTime(fp.order_purchase_timestamp)))) AS quarter,
    round(sum(fp.total_payment), 2) AS hv_revenue
FROM analytics.fact_payments fp
JOIN analytics.customer_segments cs ON cs.customer_id = fp.customer_id
WHERE cs.total_spent >= 200
AND fp.order_status = 'delivered'
AND fp.order_purchase_timestamp != ''
GROUP BY quarter
ORDER BY quarter
```

**Q14 — Order Volume by Quarter**
```sql
SELECT
    concat('Q', toString(toQuarter(toDateTime(order_purchase_timestamp))),
           '-', toString(toYear(toDateTime(order_purchase_timestamp)))) AS quarter,
    count(*) AS total_orders
FROM analytics.fact_payments
WHERE order_status = 'delivered'
AND order_purchase_timestamp != ''
GROUP BY quarter
ORDER BY quarter
```

**Q18 — HV Repeat Purchase Rate**
```sql
SELECT
    cs.segment,
    countIf(order_count > 1) AS repeat_customers,
    count(*) AS total_customers,
    round(countIf(order_count > 1) * 100.0 / count(*), 2) AS repeat_rate_pct
FROM analytics.customer_segments cs
JOIN (
    SELECT customer_unique_id, count(*) AS order_count
    FROM analytics.fact_payments
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
) ord ON cs.customer_unique_id = ord.customer_unique_id
GROUP BY cs.segment
ORDER BY repeat_rate_pct DESC
```

**Q23a — Transaction Volume by Day of Week**
```sql
SELECT
    toDayOfWeek(toDateTime(order_purchase_timestamp)) AS day_of_week,
    count(*) AS all_orders,
    countIf(customer_id IN (
        SELECT customer_id FROM analytics.customer_segments WHERE total_spent >= 200
    )) AS hv_orders
FROM analytics.fact_payments
WHERE order_purchase_timestamp != ''
AND order_status = 'delivered'
GROUP BY day_of_week
ORDER BY day_of_week
```

**Q23b — Transaction Volume by Hour**
```sql
SELECT
    toHour(toDateTime(order_purchase_timestamp)) AS hour_of_day,
    count(*) AS all_orders,
    countIf(customer_id IN (
        SELECT customer_id FROM analytics.customer_segments WHERE total_spent >= 200
    )) AS hv_orders
FROM analytics.fact_payments
WHERE order_purchase_timestamp != ''
AND order_status = 'delivered'
GROUP BY hour_of_day
ORDER BY hour_of_day
```

---

### Payment Behaviour & Preferences

**Q3 — Payment Method by Segment (Stacked Bar)**
```sql
SELECT
    cs.segment,
    countIf(fp.payment_type = 'credit_card') AS credit_card,
    countIf(fp.payment_type = 'boleto')      AS boleto,
    countIf(fp.payment_type = 'debit_card')  AS debit_card,
    countIf(fp.payment_type = 'voucher')     AS voucher
FROM analytics.customer_segments cs
JOIN analytics.fact_payments fp ON cs.customer_id = fp.customer_id
WHERE fp.order_status = 'delivered'
GROUP BY cs.segment
ORDER BY cs.segment
```

**Q3.5 — Payment Method Distribution (Premium % Revenue)**
```sql
SELECT
    cs.segment,
    fp.payment_type,
    COUNT(DISTINCT fp.order_id) AS total_orders,
    ROUND(SUM(fp.total_payment), 2) AS total_revenue,
    ROUND(SUM(fp.total_payment) / SUM(SUM(fp.total_payment)) OVER (PARTITION BY cs.segment) * 100, 2) AS revenue_pct
FROM analytics.fact_payments fp
JOIN analytics.customer_segments cs
    ON fp.customer_unique_id = cs.customer_unique_id
WHERE cs.segment IN ('Premium')
AND fp.order_status = 'delivered'
GROUP BY cs.segment, fp.payment_type
ORDER BY cs.segment, total_revenue DESC
```

**Q4 — HV Installment Distribution (CC Only)**
```sql
SELECT
    fp.max_installments,
    count(*) AS total_orders
FROM analytics.fact_payments fp
JOIN analytics.customer_segments cs ON cs.customer_id = fp.customer_id
WHERE cs.total_spent >= 200
AND fp.payment_type = 'credit_card'
AND fp.order_status = 'delivered'
GROUP BY fp.max_installments
ORDER BY fp.max_installments
```

**Q9 — Avg Order Value: Single vs Installment**
```sql
SELECT
    CASE
        WHEN max_installments = 1 THEN '1x (Single Payment)'
        ELSE '2x+ (Installment)'
    END AS installment_group,
    round(avg(total_payment), 2) AS avg_order_value,
    count(*) AS total_orders
FROM analytics.fact_payments
WHERE order_status = 'delivered'
GROUP BY installment_group
ORDER BY avg_order_value DESC
```

**Q16 — Installments vs Avg Order Value (1–10x, CC Only)**
```sql
SELECT
    max_installments,
    round(avg(total_payment), 2) AS avg_order_value,
    count(*) AS total_orders
FROM analytics.fact_payments
WHERE payment_type = 'credit_card'
AND order_status = 'delivered'
AND max_installments BETWEEN 1 AND 10
GROUP BY max_installments
ORDER BY max_installments
```

**B1 — Repeat Order Rate: Voucher vs Non-Voucher**
```sql
SELECT
    voucher_user,
    countIf(order_count > 1) AS repeat_customers,
    count(*) AS total_customers,
    round(countIf(order_count > 1) * 100.0 / count(*), 2) AS repeat_rate_pct
FROM (
    SELECT
        customer_unique_id,
        maxIf(1, payment_type = 'voucher') = 1 ? 'Voucher User' : 'Non-Voucher' AS voucher_user,
        count(DISTINCT order_id) AS order_count
    FROM analytics.fact_payments
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
)
GROUP BY voucher_user
ORDER BY repeat_rate_pct DESC
```

**B2 — Avg Order Value: With Voucher vs Without**
```sql
SELECT
    CASE
        WHEN payment_type = 'voucher' THEN 'With Voucher'
        ELSE 'Without Voucher'
    END AS voucher_group,
    round(avg(total_payment), 2) AS avg_order_value,
    count(*) AS total_orders
FROM analytics.fact_payments
WHERE order_status = 'delivered'
GROUP BY voucher_group
ORDER BY avg_order_value DESC
```

**B3 — Voucher Usage by Segment**
```sql
SELECT
    cs.segment,
    countIf(fp.payment_type = 'voucher') AS voucher_orders,
    count(*) AS total_orders,
    round(countIf(fp.payment_type = 'voucher') * 100.0 / count(*), 2) AS voucher_pct
FROM analytics.customer_segments cs
JOIN analytics.fact_payments fp ON cs.customer_id = fp.customer_id
WHERE fp.order_status = 'delivered'
GROUP BY cs.segment
ORDER BY voucher_pct DESC
```

---

### Geographic Distribution

**Q5 — Top 8 States by HV Revenue**
```sql
SELECT
    cs.customer_state,
    round(sum(fp.total_payment), 2) AS hv_revenue
FROM analytics.customer_segments cs
JOIN analytics.fact_payments fp ON cs.customer_id = fp.customer_id
WHERE cs.total_spent >= 200
AND fp.order_status = 'delivered'
GROUP BY cs.customer_state
ORDER BY hv_revenue DESC
LIMIT 8
```

**Q6 — HV Customer Concentration by State (%)**
```sql
SELECT
    customer_state,
    countIf(total_spent >= 200) AS hv_count,
    count(*) AS total_count,
    round(countIf(total_spent >= 200) * 100.0 / count(*), 2) AS hv_pct
FROM analytics.customer_segments
GROUP BY customer_state
ORDER BY hv_pct DESC
LIMIT 15
```

**Q17 — State Opportunity Matrix (Scatter)**
```sql
SELECT
    customer_state,
    round(sumIf(total_spent, total_spent >= 200), 2) AS hv_revenue,
    round(countIf(total_spent >= 200) * 100.0 / count(*), 2) AS hv_concentration_pct,
    count(*) AS total_customers
FROM analytics.customer_segments
GROUP BY customer_state
HAVING total_customers >= 50
ORDER BY hv_revenue DESC
```

**Q17.5 — Region with High HV Revenue but Low Installment Usage**
```sql
SELECT
    rbs.customer_state,
    rbs.total_revenue AS hv_revenue,
    rbs.unique_customers AS hv_customers,
    ROUND(rbs.total_revenue / SUM(rbs.total_revenue) OVER () * 100, 2) AS hv_revenue_pct,
    ROUND(AVG(fp.max_installments), 2) AS avg_installments,
    CASE
        WHEN rbs.total_revenue > 500000 AND AVG(fp.max_installments) < 3
            THEN 'High Priority'
        WHEN rbs.total_revenue > 200000 AND AVG(fp.max_installments) < 3
            THEN 'Medium Priority'
        ELSE 'Low Priority'
    END AS opportunity_tier
FROM analytics.revenue_by_state rbs
JOIN analytics.fact_payments fp ON rbs.customer_state = fp.customer_state
JOIN analytics.customer_segments cs ON fp.customer_unique_id = cs.customer_unique_id
WHERE cs.segment IN ('Premium', 'High')
AND fp.payment_type = 'credit_card'
AND fp.order_status = 'delivered'
GROUP BY rbs.customer_state, rbs.total_revenue, rbs.unique_customers
ORDER BY hv_revenue DESC
```

---

### Revenue Leakage & Recommendations

**Q15 — Total Revenue at Risk (Gauge)**
```sql
SELECT round(sum(total_payment), 2) AS total_revenue_at_risk
FROM analytics.revenue_leakage
```

**Q7 — Voucher Revenue at Risk (Number Card)**
```sql
SELECT
    'Voucher at Risk' AS label,
    SUM(total_payment) AS value
FROM analytics.revenue_leakage
WHERE payment_type = 'voucher'
AND order_status = 'canceled'
UNION ALL
SELECT
    'Total Revenue' AS label,
    SUM(total_payment) AS value
FROM analytics.fact_payments
WHERE order_status = 'delivered'
```

**Q7a — Voucher Canceled Value**
```sql
SELECT round(sum(total_payment), 2) AS voucher_canceled_value
FROM analytics.revenue_leakage
WHERE payment_type = 'voucher'
AND order_status = 'canceled'
```

**Q7b — % of HV Revenue**
```sql
SELECT round(
    (SELECT sum(total_payment) FROM analytics.revenue_leakage WHERE payment_type = 'voucher' AND order_status = 'canceled')
    /
    (SELECT sum(fp.total_payment) FROM analytics.fact_payments fp
     JOIN analytics.customer_segments cs ON fp.customer_id = cs.customer_id
     WHERE cs.total_spent >= 200 AND fp.order_status = 'delivered')
    * 100, 2) AS pct_of_hv_revenue
```

**Q8 — Cancel Rate by Payment Type**
```sql
SELECT
    payment_type,
    countIf(order_status = 'canceled') AS canceled_orders,
    count(*) AS total_orders,
    round(countIf(order_status = 'canceled') * 100.0 / count(*), 2) AS cancel_rate_pct
FROM analytics.fact_payments
GROUP BY payment_type
ORDER BY cancel_rate_pct DESC
```

**Q8.5 — Revenue Lost from Cancelled Orders by Payment Type**
```sql
SELECT
    payment_type,
    COUNT(DISTINCT order_id) AS cancelled_orders,
    ROUND(SUM(total_payment), 2) AS revenue_lost,
    ROUND(AVG(total_payment), 2) AS avg_lost_per_order,
    ROUND(
        SUM(total_payment) / SUM(SUM(total_payment)) OVER () * 100
    , 2) AS pct_of_total_lost
FROM analytics.revenue_leakage
WHERE order_status = 'canceled'
GROUP BY payment_type
ORDER BY revenue_lost DESC
```

**A1 — Cancel Rate by Payment Type × Order Value Range**
```sql
SELECT
    payment_type,
    CASE
        WHEN total_payment < 100  THEN 'Low (<R$100)'
        WHEN total_payment < 300  THEN 'Mid (R$100-300)'
        ELSE 'High (>R$300)'
    END AS value_range,
    countIf(order_status = 'canceled') AS canceled,
    count(*) AS total,
    round(countIf(order_status = 'canceled') * 100.0 / count(*), 2) AS cancel_rate_pct
FROM analytics.fact_payments
GROUP BY payment_type, value_range
ORDER BY cancel_rate_pct DESC
```

**A4 — Cancel Rate by State**
```sql
SELECT
    fp.customer_state,
    countIf(fp.order_status = 'canceled') AS canceled_orders,
    count(*) AS total_orders,
    round(countIf(fp.order_status = 'canceled') * 100.0 / count(*), 2) AS cancel_rate_pct
FROM analytics.fact_payments fp
GROUP BY fp.customer_state
ORDER BY cancel_rate_pct DESC
LIMIT 15
```

**C1 — High-Price Categories with CC 1x (Missed Upsell)**
```sql
SELECT
    i.category,
    count(*) AS total_orders,
    round(avg(i.price), 2) AS avg_price
FROM analytics.order_items_products i
JOIN analytics.fact_payments fp ON i.order_id = fp.order_id
WHERE fp.payment_type = 'credit_card'
AND fp.max_installments = 1
AND fp.order_status = 'delivered'
AND i.price >= 150
GROUP BY i.category
ORDER BY total_orders DESC
LIMIT 10
```

**C3 — Near-Threshold Customers Using CC 1x (Most Actionable)**
```sql
SELECT
    count(DISTINCT fp.customer_id) AS near_threshold_cc_1x_customers,
    count(*) AS total_orders,
    round(avg(fp.total_payment), 2) AS avg_order_value
FROM analytics.fact_payments fp
JOIN analytics.customer_segments cs ON fp.customer_id = cs.customer_id
WHERE cs.total_spent BETWEEN 150 AND 200
AND fp.payment_type = 'credit_card'
AND fp.max_installments = 1
AND fp.order_status = 'delivered'
```

**Q21 — Revenue Upgrade Scenario**
```sql
SELECT 'Convert 10%' AS scenario,
    round(count(*) * 0.10 * 200, 2) AS projected_revenue
FROM analytics.customer_segments WHERE total_spent BETWEEN 150 AND 200
UNION ALL
SELECT 'Convert 25%' AS scenario,
    round(count(*) * 0.25 * 200, 2) AS projected_revenue
FROM analytics.customer_segments WHERE total_spent BETWEEN 150 AND 200
UNION ALL
SELECT 'Convert 50%' AS scenario,
    round(count(*) * 0.50 * 200, 2) AS projected_revenue
FROM analytics.customer_segments WHERE total_spent BETWEEN 150 AND 200
UNION ALL
SELECT 'Convert 100%' AS scenario,
    round(count(*) * 1.00 * 200, 2) AS projected_revenue
FROM analytics.customer_segments WHERE total_spent BETWEEN 150 AND 200
```

---

