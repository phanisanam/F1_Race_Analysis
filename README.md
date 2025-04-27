Formula -1 Analytics: - 
• Tools & Technologies: Azure Data Factory, Azure Data Lake, Azure Databricks, PySpark, Delta Lake, JSON, CSV, SQL • 
The dataset was sourced from a GitHub repository and contains historical Formula 1 data, including information on drivers, constructors, circuits, races. 
• Implemented the Medallion Architecture (Bronze, Silver, Gold) to structure data processing layers in Azure Data Lake.
• Developed a dynamic and robust data pipeline using Azure Data Factory and Azure Data Lake to extract drivers, constructors, races, circuits (raw data) from the GitHub API. 
The pipeline was designed to intelligently detect and process multiple file formats within a single unified workflow. 
• Raw Layer: - Extract the data from API and Storing the raw data in data lake.
• Silver Layer: - Ensured high data quality by implementing validation checks, such as handling the null values in driver data and constructor’s data including schema validation and converting the data into delta format • Implemented incremental data loading using Delta Lake’s upsert functionality, reducing processing time and significantly enhancing query performance through Delta’s ACID transactions, schema enforcement, and data versioning. 
• Gold Layer: - Perform the Aggregations and joins to generate the dashboard data to represent F1-results such as race results and driver standings. 
