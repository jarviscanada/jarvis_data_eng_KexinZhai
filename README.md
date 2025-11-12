# Kexin Zhai . Jarvis Consulting

I studied at the University of Toronto (BSc, then MEng focused on analytics/ML), and I?m a data/analytics-oriented engineer with recent experience building small end-to-end systems and doing applied machine learning. On the technical side I work most comfortably in Python, Java, Linux/Bash, and SQL/RDBMS, and I?ve used Docker, PySpark, Databricks, and Azure for data workloads. In school and personal projects I?ve built things across the stack: a Linux cluster monitor that ingests host metrics into PostgreSQL with Bash/Docker; quantitative and ML projects in Python (ETF momentum, equity-return forecasting, deepfake detection); and several database / web apps where I designed schemas and wired up CRUD logic. My work experience backs that up: at GiftCash I worked with real marketplace data, pricing tools, SellerCloud, and repricing workflows; at the Ministry of Education I ran full QA cycles with Selenium, HP ALM, PL/SQL, and Azure DevOps to keep releases clean. I like staying in tech spaces that are practical, collaborative, and data-first. I care about showing I can execute, not just list tools.

## Skills

**Proficient:** Python, Java, Linux/Bash, RDBMS/SQL, R, Agile/Scrum, Git

**Competent:** C, Databricks, Docker, PySpark, Azure

**Familiar:** Angular, Tableau, Power BI, Spark, Hadoop, Postman, MIPS, REST APIs, CI/CD

## Jarvis Projects

Project source code: [https://github.com/jarviscanada/jarvis_data_eng_KexinZhai](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai)


**Cluster Monitor** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/linux_sql)]: Built a lightweight Linux cluster monitoring solution that collects host hardware info and minute-level resource metrics with Bash scripts and ships them to a centralized PostgreSQL instance running in Docker. Automated data ingestion with cron, standardized the schema with SQL DDL, and provided sample analytical queries for capacity planning and troubleshooting. Tech stack: Bash, Docker, PostgreSQL, cron, Git/GitFlow, Linux command-line tools (lscpu, vmstat, df, awk).

**Core Java Apps** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/core_java)]: Not Started

**Springboot App** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/springboot)]: Not Started

**Python Data Analytics** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/python_data_anlytics)]: Not Started

**Hadoop** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/hadoop)]: Not Started

**Spark** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/spark)]: Not Started

**Cloud/DevOps** [[GitHub](https://github.com/jarviscanada/jarvis_data_eng_KexinZhai/tree/master/cloud_devops)]: Not Started


## Highlighted Projects
**Adaptive Laddered ETF Momentum Strategy for Portfolio Optimization**: Built a laddered momentum ETF strategy that used time-diversified tranches to enhance returns while reducing volatility and drawdowns; implemented a 2-tranche system with staggered 2-week investments to balance momentum capture and trend responsiveness; backtested the strategy on sector ETFs (2020?2024) in Python and showed outperformance over SPY on risk-adjusted metrics (Sharpe/Sortino) with lower drawdowns; and automated weekly ETF selection plus reporting to demonstrate the laddering effectiveness for momentum portfolios.

**Innovation Strategy Development for Canada**: Guided an innovation-strategy study that compared Canada?s innovation strengths (R&D, talent, institutions) with leading countries using Global Innovation Index data; collected and cleaned fragmented country-level innovation indicators to increase dataset reliability in Python (Pandas, Seaborn); applied clustering and feature-importance modeling to identify key drivers of innovation success across 100+ indicators; and developed an AI chatbot (Langflow + LLM) to deliver interactive insights and authored actionable policy recommendations.

**Deepfake Detection with CNNs and Object Detection**: Built a deepfake-detection pipeline using CNNs (InceptionV3, Xception, etc.) trained on 190K+ real/synthetic facial images; boosted accuracy by integrating YOLOv1 for face detection before classification; optimized performance through data augmentation and hyperparameter tuning while preventing overfitting; and tested model fairness across demographic groups and synthetic media types (GANs, StyleGAN).

**Machine Learning Pipeline for Weekly Return Forecasting in Equity Markets**: Built a Python-based weekly equity-return forecasting system that enhanced a textbook model with novel volatility/wavelet features; curated multi-asset data (Yahoo Finance/FRED) and engineered a boosted pipeline with Ridge/Elastic Net/Bayesian modeling; validated the models using Sharpe Ratio, Profit Factor, and anti-overfitting techniques (bootstrapping/Monte Carlo); and delivered a 40-slide presentation plus technical notebook that showed a production-ready ML workflow for finance.

**APOPO Project on Rat Odor Preference Behavior for Landmine Detection**: Partnered with APOPO to analyze rat-training video data and assessed how early exposure affects odor-preference behavior for landmine detection; processed large volumes of video-derived behavioral metrics to ensure reliability before statistical modeling; used PCA and mixed-effects models to identify key training factors and optimized model selection for interpretability; and delivered training recommendations and co-authored a report to enhance APOPO?s conditioning program.

**Data Analysis Project on Systolic Blood Pressure**: Cleaned and validated a systolic-blood-pressure dataset to remove errors and irrelevant records prior to analysis; explored variable relationships using R-generated visualizations (boxplots, heatmaps, etc.) to identify key SBP influencers; validated the chosen model through statistical diagnostics (LINE assumptions, outlier detection) and coordinated team tasks; and presented findings in a structured PowerPoint and script that explained SBP determinants to the class.

**Bullet Journal Webpage Application** [[GitHub](https://github.com/UTSCCSCC01/finalprojects22-cyclist)]: Built a bullet-journal web app using Angular and Agile/Scrum, finishing frontend tasks from the backlog so the UI stood out from other diary apps; conducted user-centric testing to keep the features aligned with stakeholder standards and reported issues to the team for timely fixes; and tracked project progress with CI/CD tools, shared workflows, and collaboration with frontend/QA teams via Zoom, GitHub, and Discord.

**MyBnB Database Design Project** [[GitHub](https://github.com/tianpai/CSCC43-project-2022-summer)]: Simulated core MyBnB operations (booking, rating, etc.) in SQL to better practice relational modeling and understand how real apps leverage databases; designed and developed a rental platform schema with normalized Java + SQL access so users could interact with the formulated database; and led the team by assigning tasks and supervising delivery to make sure features were finished in time for integration testing.


## Professional Experiences

**Junior Pricing Analyst, GiftCash (September 2023 - December 2023)**: Leveraged discounted gift cards and product-research tools such as JungleScout and Keepa to identify high-ROI Amazon products, and analyzed Buybox competition to determine optimal FBA/FBM strategies using a custom ROI calculator; managed product data in SellerCloud and coordinated online/offline purchasing teams to keep inventory at optimal levels while processing real-time buying decisions based on profitability analysis; monitored and repriced Amazon listings through Repricer, managed eBay listings with cross-platform price synchronization, and liaised with warehouse teams to ensure fulfillment compliance across both marketplaces.

**IT Quality Assurance Assistant, Ministry of Education (September 2021 - April 2022)**: Performed manual and automated testing for Ministry of Education applications using Selenium, HP ALM, and PL/SQL Developer, ensuring defect-free production releases while managing multiple projects and maintaining communication with cross-functional teams; executed the full testing lifecycle?requirements review, test planning with traceability matrices, test-case execution in HP ALM, and results analysis?while using Azure DevOps to log defects and follow up with development teams for resolution; mastered HP ALM, PL/SQL Developer, and Selenium frameworks to optimize testing efficiency, implemented automated regression testing, and contributed to process improvements through DevOps collaboration and documentation standardization.


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
- IBM Business Analyst Work Placement Education Program: Digital Transformation?The Road to Future-Ready Organizations
- Vice President, Chinese Students and Scholars Association at University of Toronto Scarborough: Negotiated sponsorships, drafted contracts, led directors in task delegation, and fostered sponsor relations to boost mutual recognition.
- Volunteer Leader, Math in Motion?Girls in Gear!: Guided the new student leaders to take action within groups harmoniously in order to fulfill their duties successfully; coached leaders to learn new design tools quickly and efficiently, especially if they were lagging behind before big events