# 🚀 Medallion Data Warehouse: From Chaos to Clarity

> "Data is the new oil, but only if it's refined." This project is a journey of discipline and technical growth—a complete end-to-end Data Warehouse solution built from the ground up, transforming raw, messy CSV files into a structured Gold-standard Star Schema.

---

## 🌟 The Vision
The goal was clear: Take fragmented data from two different sources (**CRM** and **ERP**) and build a robust, automated pipeline. This project implements the **Medallion Architecture**, ensuring data quality at every step:

* **Bronze:** Raw ingestion of source files.
* **Silver:** Cleaning, standardizing, and reconciling data.
* **Gold:** Strategic modeling for BI and Analytics.

---

## 🏗️ Project Architecture & Folder Structure
Organization is the key to scalability. Here is how the project is structured:

```text
dwh_database/ 
├── data/               # Source CSV files (CRM & ERP) + Application Logs
├── sql_scripts/
│   ├── ddl/            # Database and Schema initialization scripts
│   ├── silver/         # Transformation logic (Cleaning & Harmonization)
│   │   ├── crm/        # CRM-specific staging scripts
│   │   └── erp/        # ERP-specific staging scripts
│   └── gold/           # Final Star Schema (Dimensions & Facts)
├── src/                # Python Core: Database utilities & Orchestration logic
├── main.py             # The Maestro: One-click pipeline execution
├── .env                # Secure Environment variables (ignored by git)
└── requirements.txt    # Project dependencies

---

🛠️ Tech Stack & Key Features

    ◉ Python & SQLAlchemy: For automated orchestration and database connectivity.

    ◉ SQL Server (T-SQL): The powerhouse for data transformations and modeling.

    ◉ Medallion Architecture: Three-layer approach for superior data quality.

    ◉ Data Reconciliation: Advanced logic to fix missing prices, quantities, and dates.

    ◉ SCD Type 2 Ready: Managing product history and active versions.

---

🚀 How to Run the Pipeline

    1. Clone the Repository.

    2. Configure Environment: Create a .env file with your SQL Server credentials.

    3. Setup Database: Execute /sql_scripts/ddl/db_and_schemas.sql.

    4. Execute: Simply run:
    python main.py

    5. Monitor: Check the data/app.log to see the live progress of each phase.

---

📈 The Journey So Far (Version 1.0)

This project represents more than just technical skills; it represents discipline. Every line of code was written with a focus on:
    ◉ Quality: No "garbage" data enters the Gold layer.

    ◉ Consistency: Harmonizing different systems (CRM vs ERP) into a single source of truth.

    ◉ Professionalism: Following industry standards in naming, documentation, and folder structure.

---

🔮 What’s Next?

The journey doesn't stop here. Version 2.0 is already on the horizon:
    ◉ ☁️ Cloud Integration: Moving the warehouse to a cloud environment.
    
    ◉ ⚙️ Airflow Orchestration: Transitioning from simple Python scripts to advanced workflow management.
    
    ◉ 📊 Dashboarding: Connecting the Gold layer to Power BI for visual storytelling.

---

✍️ About the Author

Abdulelah – A Data Engineer in the making, driven by curiosity and fueled by discipline. This project is just the beginning.
"I am coming, and the best is yet to be, by the will of Allah."
