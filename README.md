# Kexin Zhai . Jarvis Consulting

I studied at the University of Toronto (BSc, then MEng focused on analytics/ML), and I'm a data/analytics-oriented engineer with hands-on experience in enterprise ETL pipelines, data integration, quality assurance, and applied machine learning. On the technical side, I work most comfortably in Python, Java, Linux/Bash, and SQL/RDBMS, with experience using PySpark, Databricks, Airflow, NiFi, Docker, and Azure for data workloads. I have built end-to-end systems including quantitative and ML applications in Python, and database/web apps with designed schemas and CRUD logic. At RBC Wealth Management, I maintained dozens of daily ETL pipelines, developed new data workflows, integrated Collibra REST APIs with PostgreSQL using SCD Type 2, and performed data quality validation, troubleshooting, and testing within enterprise SDLC and change management processes. My previous experience includes marketplace data analysis, pricing tools, SellerCloud, and repricing workflows at GiftCash, as well as full QA cycles using Selenium, HP ALM, PL/SQL, and Azure DevOps at the Ministry of Education. I enjoy practical, collaborative, data-first environments and focus on demonstrating execution, not just listing tools.

## Skills

**Proficient:** Python, PySpark, Databricks, SQL/RDBMS (PostgreSQL), R, Java, Git/GitHub, Apache Airflow, Apache NiFi, HashiCorp Vault, ETL/ELT Pipelines, Postman, REST API Integration, AI/NLP (LLMs, Multi-agent Architecture, OpenAI API, Prompt Engineering), Machine/Deep Learning (TensorFlow, PyTorch), Confluence, Lucidchart, Agile/Scrum, LaTeX, MATLAB, MS Office, G Suite

**Competent:** C, Starburst, Collibra, Data Modeling (SCD Type 2), Data & Cloud Analysis, Data Quality & Validation, Software QA & Automated Testing (pytest, Selenium, JUnit), Docker, Cloud Computing (AWS, Azure, GCP)

**Familiar:** Angular, Front-end Development (HTML/CSS, JavaScript, Bootstrap), Tableau, Power BI, Hadoop, MIPS, Linux/Bash, CI/CD

## Jarvis Projects

Project source code: [https://github.com/jarviscanada/jarvis_data_eng_KexinZhai](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai)


**Cluster Monitor** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/linux_sql)]: Built a lightweight Linux cluster monitoring solution that collects host hardware info and minute-level resource metrics with Bash scripts and ships them to a centralized PostgreSQL instance running in Docker. Automated data ingestion with cron, standardized the schema with SQL DDL, and provided sample analytical queries for capacity planning and troubleshooting. Tech stack: Bash, Docker, PostgreSQL, cron, Git/GitFlow, Linux command-line tools (lscpu, vmstat, df, awk).

**Core Java Apps** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/core_java)]: Built a Java CLI grep application using Maven/IntelliJ, applying black-box testing to validate demo JAR behavior (regex matching, recursive file traversal, and output generation). Implemented RegexExc for pattern validation and designed JavaGrep/JavaGrepImp interfaces and classes to orchestrate file traversal, line filtering, and output writing with SLF4J/log4j logging. Enhanced the application using Java 8 Streams/Lambdas (LambdaStreamExc/Imp, JavaGrepLambdaImp), replacing loops with Files.walk/Files.lines. Packaged with Maven Shade, containerized using Docker (OpenJDK 8), tested with mounted data/log volumes, and published to Docker Hub. Tech stack: Java, Maven (Shade), IntelliJ, SLF4J/log4j, Java 8 Streams/Lambdas, Docker, Git/GitFlow.

**Python Data Analytics** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/python_data_anlytics)]: Built a retail analytics workflow in a Docker-based Python/Jupyter environment, connecting to PostgreSQL via psycopg2/SQLAlchemy and loading transaction data into Pandas. Performed SQL/Pandas data exploration and validation (pd.read_sql/pd.read_csv), calculated invoice totals, visualized amount distributions (full and 85th-percentile trimmed), and generated KPIs (orders, customers, SKUs, revenue, monthly sales). Extended analysis with time-series metrics (monthly orders/cancellations, sales growth, active/new/existing users) and RFM customer segmentation for retention insights. Tech stack: Python, Jupyter Notebook, Pandas, NumPy, Matplotlib, SQL, PostgreSQL, psycopg2, SQLAlchemy, Docker, Git.

**PySpark Pipeline** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/pyspark)]: Developed an end-to-end equity-market ETL pipeline ingesting daily OHLCV CSVs and company dimension data through a Raw → Bronze → Silver → Gold medallion architecture, producing analytics-ready datasets for performance/risk screening. Orchestrated Spark jobs with Airflow DAGs in Docker and implemented fail-fast data quality gates (schema/emptiness, ticker-date uniqueness, price/volume validity, trading-day completeness, sector/industry referential integrity), with audit/quarantine diagnostics for reproducibility. Generated Gold datasets with daily returns, rolling metrics, and sector/industry analytics (sector_daily_returns, industry_volatility). Tech stack: Python, PySpark, Apache Spark, Apache Airflow, Docker/Docker Compose, PostgreSQL, Parquet, exchange-calendars, SQL, pytest, Git.


## Highlighted Projects
**Adaptive Laddered ETF Momentum Strategy for Portfolio Optimization**: Built a laddered momentum ETF strategy that used time-diversified tranches to enhance returns while reducing volatility and drawdowns; implemented a 2-tranche system with staggered 2-week investments to balance momentum capture and trend responsiveness; backtested the strategy on sector ETFs (2020-2024) in Python and showed outperformance over SPY on risk-adjusted metrics (Sharpe/Sortino) with lower drawdowns; and automated weekly ETF selection plus reporting to demonstrate the laddering effectiveness for momentum portfolios.

**Innovation Strategy Development for Canada**: Guided an innovation-strategy study that compared Canada's innovation strengths (R&D, talent, institutions) with leading countries using Global Innovation Index data; collected and cleaned fragmented country-level innovation indicators to increase dataset reliability in Python (Pandas, Seaborn); applied clustering and feature-importance modeling to identify key drivers of innovation success across 100+ indicators; and developed an AI chatbot (Langflow + LLM) to deliver interactive insights and authored actionable policy recommendations.

**Deepfake Detection with CNNs and Object Detection**: Built a deepfake-detection pipeline using CNNs (InceptionV3, Xception, etc.) trained on 190K+ real/synthetic facial images; boosted accuracy by integrating YOLOv1 for face detection before classification; optimized performance through data augmentation and hyperparameter tuning while preventing overfitting; and tested model fairness across demographic groups and synthetic media types (GANs, StyleGAN).

**Machine Learning Pipeline for Weekly Return Forecasting in Equity Markets**: Built a Python-based weekly equity-return forecasting system that enhanced a textbook model with novel volatility/wavelet features; curated multi-asset data (Yahoo Finance/FRED) and engineered a boosted pipeline with Ridge/Elastic Net/Bayesian modeling; validated the models using Sharpe Ratio, Profit Factor, and anti-overfitting techniques (bootstrapping/Monte Carlo); and delivered a 40-slide presentation plus technical notebook that showed a production-ready ML workflow for finance.

**APOPO Project on Rat Odor Preference Behavior for Landmine Detection**: Partnered with APOPO to analyze rat-training video data and assessed how early exposure affects odor-preference behavior for landmine detection; processed large volumes of video-derived behavioral metrics to ensure reliability before statistical modeling; used PCA and mixed-effects models to identify key training factors and optimized model selection for interpretability; and delivered training recommendations and co-authored a report to enhance APOPO's conditioning program.

**Bullet Journal Webpage Application** [[GitHub](https://github.com/UTSCCSCC01/finalprojects22-cyclist)]: Built a bullet-journal web app using Angular and Agile/Scrum, finishing frontend tasks from the backlog so the UI stood out from other diary apps; conducted user-centric testing to keep the features aligned with stakeholder standards and reported issues to the team for timely fixes; and tracked project progress with CI/CD tools, shared workflows, and collaboration with frontend/QA teams via Zoom, GitHub, and Discord.


## Professional Experiences

**Data Engineer, RBC Wealth Management (Royal Bank of Canada) (January 2026 - October 2026)**: Collaborated with cross-functional teams to maintain dozens of daily ETL pipelines, troubleshooting production issues, resolving bugs, and performing upgrades to ensure operational stability; designed and developed end-to-end ETL pipelines using Python, SQL, Airflow, NiFi, and PySpark, translating business requirements into technical workflows and presenting designs; built metadata ingestion pipelines integrating Collibra REST APIs with PostgreSQL, implementing data transformations, SCD Type 2 modeling, and incremental data processing for historical tracking; performed data quality checks, schema validation, and troubleshooting to ensure data accuracy, consistency, and pipeline reliability; leveraged GitHub, Confluence, Lucidchart, Starburst, and Vault while following enterprise SDLC, testing, and change management practices across environments.

**Junior Pricing Analyst, GiftCash (September 2023 - December 2023)**: Leveraged discounted gift cards and product-research tools such as JungleScout and Keepa to identify high-ROI Amazon products, and analyzed Buybox competition to determine optimal FBA/FBM strategies using a custom ROI calculator; managed product data in SellerCloud and coordinated online/offline purchasing teams to keep inventory at optimal levels while processing real-time buying decisions based on profitability analysis; monitored and repriced Amazon listings through Repricer, managed eBay listings with cross-platform price synchronization, and liaised with warehouse teams to ensure fulfillment compliance across both marketplaces.

**IT Quality Assurance Assistant, Ministry of Education (September 2021 - April 2022)**: Performed manual and automated testing for Ministry of Education applications using Selenium, HP ALM, and PL/SQL Developer, ensuring defect-free production releases while managing multiple projects and maintaining communication with cross-functional teams; executed the full testing lifecycle including requirements review, test planning with traceability matrices, test-case execution in HP ALM, and results analysis, while using Azure DevOps to log defects and follow up with development teams for resolution; mastered HP ALM, PL/SQL Developer, and Selenium frameworks to optimize testing efficiency, implemented automated regression testing, and contributed to process improvements through DevOps collaboration and documentation standardization.


## Education
**University of Toronto (2024-2025)**, Master of Engineering, Mechanical & Industrial Engineering
- Technical Emphasis in Data Analytics and Machine Learning, MEng Certificate in Financial Engineering
- GPA: 3.97/4.0

**University of Toronto Scarborough (2019-2024)**, Honours Bachelor of Science (Coop) with High Distinction, Computer and Mathematical Sciences
- Specialist in Statistics (Machine Learning & Data Mining) with Major in Computer Science & Minor in Linguistics
- Dean's List (2019, 2020, 2021, 2022, 2023, 2024)
- GPA: 3.83/4.0


## Miscellaneous
- Quantium Data Analytics Virtual Experience Program on Forage
- KPMG Data Analytics Consulting Virtual Internship on Forage
- BCG Data Science & Analytics Virtual Experience Program on Forage
- IBM Business Analyst Work Placement Education Program: Digital Transformation -- The Road to Future-Ready Organizations
- Vice President, Chinese Students and Scholars Association at University of Toronto Scarborough: Negotiated sponsorships, drafted contracts, led directors in task delegation, and fostered sponsor relations to boost mutual recognition.
- Volunteer Leader, Math in Motion - Girls in Gear!: Guided the new student leaders to take action within groups harmoniously; coached leaders to learn new design tools.