# Power BI Assistant README
<img width="950" height="532" alt="power bi mcp server chat bot" src="https://github.com/user-attachments/assets/7f6a9e63-9986-4493-9da1-b76212e84762" />

## Overview

This Power BI Assistant is designed to help users interact with their Power BI datasets more effectively. It provides capabilities for querying data, creating measures, and managing data models, all through natural language interactions.
## How to Run

1.Download the code file and Power bi MCp server.
2.Place the location of the mcp server in the env file.
3.Open the power bi desktop which want to connect and run the code.

1. **Natural Language Processing**: The assistant interprets user requests and determines the appropriate action, whether it's querying data, creating measures, or managing data models.

2. **DAX and Data Model Operations**: It utilizes DAX (Data Analysis Expressions) for querying and creating measures, and it can perform operations on tables, columns, and relationships within the data model.

3. **Connection Management**: The assistant can connect to various data sources, including Power BI Desktop and Analysis Services, to access and manipulate data.

4. **Time Intelligence**: It supports time-based calculations such as Year-over-Year (YoY) and Month-over-Month (MoM) growth, using DAX functions like `SAMEPERIODLASTYEAR` and `DATEADD`.

## Capabilities

- **Data Queries**: Retrieve and display data from datasets using DAX queries.
- **Measure Creation**: Create new DAX measures for calculations like totals, averages, and time intelligence.
- **Data Exploration**: List tables, columns, and their data types.
- **Time Intelligence**: Implement time-based calculations for business insights.
- **Data Modeling**: Manage tables, columns, relationships, and hierarchies.
- **Security and Permissions**: Manage roles and permissions for data access.
- **Export and Import**: Assist with exporting and importing model definitions.
- **Connection Management**: Connect to and manage various data sources.
- **Troubleshooting**: Provide guidance on resolving common issues.

## Usage

To interact with the assistant, simply type your request in natural language. For example:
- "Show total sales"
- "Create a measure for average sales"
- "List all columns in the Sales table"
- "Connect to my Power BI dataset"

## Limitations

- The assistant cannot create or send files directly, such as PDFs.
- It requires an active connection to a data source to perform operations.

## Conclusion

This Power BI Assistant is a powerful tool for enhancing your data analysis capabilities within Power BI. By leveraging natural language processing and DAX, it simplifies complex data operations and provides valuable insights.

---
