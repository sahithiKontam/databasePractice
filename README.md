External API → Database Integration
Task Overview

This task focuses on fetching a large dataset from an external API and storing it into a PostgreSQL database in a structured manner, bruno testing.
Target: Fetch 10,000 records
Data Source: World Bank Data API
Database: PostgreSQL
Testing : bruno

1.External API Details
 API Provider: World Bank Data API 
 api:https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL
 Data Format: JSON
 Key Features Used:Query parameters,Pagination support,Rate limit considerations

Approach & Implementation
1️.API Analysis
Identified relevant World Bank API endpoints
Analyzed supported parameters, pagination mechanism, and rate limits

2️. Data Fetching Strategy
Implemented pagination-based approach
Configured requests to fetch 100 records per page
Planned iterative calls to safely retrieve ~10,000 records

3️. API Testing
Used Bruno to test API endpoints
Added required query parameters
Verified API responses and pagination behavior

4️. Database Design
Created a new PostgreSQL database using pgAdmin 4
Designed table structure based on API response fields
Prepared database schema for storing fetched records

5.Tools & Technologies

External API: World Bank Data API(https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL)

API Testing Tool: Bruno

Database: PostgreSQL

DB Management: pgAdmin 4

6.My Outcome
Successfully analyzed and tested external API

Designed a scalable pagination strategy

Created database and table structure aligned with API data

Established groundwork for programmatic data ingestion# databasePractice
