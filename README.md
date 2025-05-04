# 🎬 **Netflix Azure Data Engineering Project** 🎬

An end-to-end data engineering pipeline built on **Microsoft Azure**, leveraging **Databricks** and **Azure Data Factory** for data ingestion, transformation, and orchestration. This project utilizes the **Medallion Architecture** (Bronze → Silver → Gold) to process Netflix metadata data, providing an efficient way to handle raw, transformed, and business-ready data.

---

## ❓ **Problem**

Organizations face several challenges in modern data engineering pipelines, such as:
- **Ingesting large volumes of raw data** from multiple sources
- **Cleaning and transforming messy datasets** for reporting
- **Ensuring data consistency** across environments
- **Managing complex workflows** with scalability and flexibility
- **Securing data access** and ensuring compliance

---

## ✅ **Solution**

This project addresses these challenges by implementing the following key steps:
- 🚀 **Ingestion**: Using **Azure Data Factory** to ingest raw Netflix data into the **Bronze** layer from Azure Data Lake Storage.
- ⚙️ **Transformation**: Leveraging **Azure Databricks** and **PySpark** to clean, enrich, and structure the data in the **Silver** layer.
- 🧠 **Storage**: Storing cleaned and structured data in **Delta Lake** format in the **Gold** layer for efficient querying and analytics.
- 🔐 **Security**: Using **Azure Active Directory** for secure access control and service communication.
- 🧩 **Orchestration**: Creating a fully automated data pipeline using **Azure Data Factory** and **Databricks Workflows**.

---

## 🏗️ **Architecture Overview**

![Netflix Azure Data Engineering Architecture](https://raw.githubusercontent.com/girald02/Azure-Netflix-Project-4/refs/heads/main/img/Azure_Netflix_Project_Architecture.png)

---

## 📊 **Data Source**

- **Netflix Metadata**: The raw data consists of various CSV files with metadata about Netflix content, such as show titles, categories, cast, directors, and countries.
  - **Sample Datasets**:
    - `netflix_cast.csv`
    - `netflix_category.csv`
    - `netflix_countries.csv`
    - `netflix_directors.csv`

---

## 🧩 **Tech Stack**

| Tool                     | Purpose                                     |
|--------------------------|---------------------------------------------|
| **Azure Data Factory**    | Ingest raw data and orchestrate pipelines   |
| **Azure Data Lake Gen2**  | Store raw and transformed data in **Bronze**, **Silver**, and **Gold** layers |
| **Azure Databricks**      | Transform data using **PySpark**            |
| **Delta Lake**            | Enable versioning, time travel, and efficient querying |
| **Databricks Unity Catalog** | Data governance and metadata management |
| **Azure Active Directory** | Secure access control and authentication   |

---

## ✅ **Project Phases**

### 🚀 **Phase 1: Ingestion with Azure Data Factory (Bronze Layer)**

- Built dynamic **Azure Data Factory** pipelines to ingest raw Netflix data into the **Bronze** layer.
- Data is stored in **CSV** format in **Azure Data Lake Storage Gen2**.
- Used **parameterized pipelines** for scalability and flexibility.

---

### ⚙️ **Phase 2: Transformation with Azure Databricks (Silver Layer)**

- Connected **Azure Databricks** to **Azure Data Lake** via **Access Connector** and **App Registration**.
- Cleaned and enriched raw Netflix metadata using **PySpark** in Databricks notebooks.
- Stored the transformed data in the **Silver** layer for further analysis.

---

### ⚙️ **Phase 3: Delta Lake – Gold Layer**

- Transformed the data into **Delta Lake** format, enabling features like **schema enforcement**, **versioning**, and **time travel**.
- Ensured data consistency and compliance using **Delta Live Tables**.
- Stored business-ready data in the **Gold** layer for consumption by downstream analytics or reporting systems.

---

### 📊 **Phase 4: Data Orchestration & Monitoring**

- Used **Databricks Workflows** to automate data processing tasks.
- Integrated **Azure Data Factory** for seamless orchestration and scheduling.
- Set up **Alerts & Notifications** using **Azure Monitoring** to track pipeline failures and successes.

---

## 🔐 **Security & Access Control**

- Secured access to **Azure Data Factory**, **Databricks**, and **Azure Data Lake** using **Azure Active Directory** and **OAuth** authentication.
- Applied **role-based access control** (RBAC) through **Azure IAM** to assign proper permissions (e.g., **Storage Blob Data Contributor**).
- Used **Access Connectors** in Databricks to securely connect to data storage resources.

---

## 🙌 **Acknowledgments**

> 🎓 **Special thanks to [Ansh Lamba](https://github.com/anshlambagit)** for his detailed tutorials that helped guide this project. His teachings are an excellent resource for anyone interested in **Azure Data Engineering**.

---

## 📌 **Key Features Implemented**

- ✅ **Parameterized pipelines** in **Azure Data Factory** for dynamic data ingestion  
- ✅ **Medallion Architecture** (Bronze, Silver, Gold) for data processing and storage  
- ✅ **PySpark** transformations in **Azure Databricks**  
- ✅ **Delta Lake** storage with **schema enforcement**, **versioning**, and **time travel**  
- ✅ **Azure Active Directory** integration for **secure data access**  
- ✅ Automated data pipeline orchestration using **Databricks Workflows**  
- ✅ **Azure Monitoring** for **alerts** and **notifications**  

---

## 📎 **Useful Links**

- [GitHub Repository](https://github.com/yourusername/Netflix-Azure-Data-Engineering-Project)  
- [Azure Data Factory Documentation](https://learn.microsoft.com/en-us/azure/data-factory/introduction)  
- [Databricks Documentation](https://docs.databricks.com/)

---

## 🤝 **Let’s Connect!**

Feel free to open issues, contribute, or connect with me on [LinkedIn](https://www.linkedin.com/in/girald-bacongan-988144174/).  
I'm always open to feedback and collaboration!
