# SQL Data Warehouse Project

## 📌 Project Overview
This repository contains a complete end-to-end **SQL Data Warehouse** implementation. The goal of this project is to transform raw operational data into a structured analytical format (Star Schema) to enable efficient business reporting and data-driven decision-making.

## 🏗️ Architecture & Modeling
The project follows a standard ETL (Extract, Transform, Load) process to move data from a landing zone to a structured warehouse.

* **Schema Type:** Star Schema
* **Fact Tables:** (e.g., Sales, Inventory, Transactions)
* **Dimension Tables:** (e.g., Customers, Products, Date, Location)

## 📁 Repository Structure
The project is organized as follows:
* **`dataset/`**: Contains the raw source files (CSV/JSON) used for ingestion.
* **`scripts/`**: SQL scripts for Database Definition (DDL) and Data Manipulation (DML).
* **`docs/`**: Documentation including the Entity-Relationship Diagram (ERD) and data dictionary.
* **`tests/`**: Quality assurance scripts to check for nulls, duplicates, and referential integrity.

## 🚀 Getting Started

### Prerequisites
- A SQL Database engine (PostgreSQL, MySQL, or SQL Server).
- A database client like DBeaver, Azure Data Studio, or pgAdmin.

### Setup Instructions
1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/charianks-alt/sql-data-warehouse-project.git](https://github.com/charianks-alt/sql-data-warehouse-project.git)
    ```
2.  **Create the Schema:**
    Run the scripts located in `/scripts` to generate the tables.
3.  **Load Data:**
    Import the files from the `/dataset` folder into your staging tables.
4.  **Verify Installation:**
    Run the scripts in the `/tests` folder to ensure data was loaded correctly.

## 📊 Key Insights
This warehouse is designed to answer business questions such as:
1.  What are the top 10 performing products by revenue?
2.  How do sales trends vary across different quarters?
3.  Which customer segments have the highest retention rates?

## 🛠️ Technologies Used
- **SQL:** Core data transformation and modeling.
- **Git:** Version control.
- **[Your DB Tool]:** (e.g., PostgreSQL / Snowflake / SQL Server)

---
Developed by [charianks-alt](https://github.com/charianks-alt).
