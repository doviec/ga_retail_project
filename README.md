# Google Analytics 360 – End-to-End Data Project  

## 📌 Overview  
This project demonstrates an **end-to-end data pipeline and analytics workflow** built on **dbt + BigQuery + Looker Studio**.  
It uses the **Google Analytics 360 sample dataset (Google Merchandise Store)** to simulate how a real-world company could track and analyze e-commerce performance.  

The project covers:  
- Ingesting **external FX rates** for revenue normalization.  
- Transforming raw Google Analytics session data into **clean fact & dimension tables**.  
- Building a **semantic analytics layer** with dbt models (staging → intermediate → marts).  
- Designing an **interactive Looker Studio dashboard** for stakeholders.  

---

## 🛠️ Tech Stack  
- **Data Warehouse**: BigQuery  
- **Transformation**: dbt (Data Build Tool)  
- **Visualization**: Looker Studio  
- **Data Source**: Google Analytics 360 Sample (Google Merchandise Store)  

---

## 🗂️ Data Modeling  
The dbt project follows the **Medallion architecture** (Bronze → Silver → Gold):  

### **Staging (stg_)**  
- `stg_sessions`: cleans GA session-level data.  
- `stg_hits_products`: standardizes product-related events.  

### **Intermediate (int_)**  
- `int_sessions_enriched`: joins sessions with products and FX rates for normalized revenue.  

### **Marts (dim_ / fct_)**  
- `dim_products`: product catalog (SKU, category, price).  
- `dim_visitors`: visitor metadata (first seen, last seen, source).  
- `fct_sales_item`: atomic fact table for transactions.  

### **Views (vw_)**  
- `vw_sales_by_date`  
- `vw_sales_by_country`  
- `vw_sales_by_product`  

---

## 📊 Dashboard  
👉 [View the Looker Studio Dashboard](https://lookerstudio.google.com/reporting/b806d875-068d-42c0-bf27-b2b18871746e)  

The dashboard is divided into two pages:  

### **Page 1 – Revenue & Sales Performance**  
- **KPIs**: Total Revenue, Transactions, Avg. Revenue per Transaction, Top Country.  
- **Charts**:  
  - Daily Revenue Trend (time series)  
  - Revenue per Country (table)  
  - Top Products by Revenue & Quantity (table)  
  - Revenue by Traffic Source (pie chart)  

### **Page 2 – Visitor Behavior & Retention**  
- **KPIs**: Total Visitors, Avg. Revenue per Visitor, Avg. Transactions per Visitor.  
- **Charts**:  
  - New vs Returning Visitors (pie chart)  
  - Revenue by Visitor Type (bar chart)  
  - Last Seen Date Trend (time series)  
  - Transactions by Campaign (bar chart)  


