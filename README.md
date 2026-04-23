# Effect of Congestion Pricing Policy on For Hire Vehicles’ Mobility in New York City

## Authors 

[Ibadat Jarg](https://www.linkedin.com/in/ibadat-jarg-200807160/), [Josahn Oginga](https://www.linkedin.com/in/josahn-oginga/), [Joya Wheatfall-Melvin](https://www.linkedin.com/in/joya-wheatfall-melvin/), [Muhammad Saad](https://www.linkedin.com/in/muhammad-saad-pp/)

## Project Site

🌐 [Visit the Project Live Site ](https://jwheatfallmel.github.io/cpz-massive-data-project/)

## Overview
Starting January 5, 2025, New York City implemented a congestion pricing policy that charges vehicles entering a designated Congestion Pricing Zone (CPZ). This zone includes Manhattan south of and including 60th Street, with some highway exemptions.

This project analyzes the **causal effect of congestion pricing on taxi and for-hire vehicle mobility patterns**, focusing on changes in **origin-destination trip volumes**.

## Research Question
What is the causal effect of congestion pricing on taxi and for-hire vehicle trips entering and leaving the CPZ?

## Data
The `data/` folder contains:

- Green taxi trip data (Parquet format):
  - `green_tripdata_2024-01.parquet`
  - `green_tripdata_2024-06.parquet`
  - `green_tripdata_2024-12.parquet`
  - `green_tripdata_2025-01.parquet`
  - `green_tripdata_2025-06.parquet`
- `taxi_zone_lookup.csv`: Lookup table for taxi zones and location IDs

These datasets allow for **pre- and post-policy comparison**.

## Exploratory Data Analysis
The `eda/` folder includes:

- `green_taxi_eda.ipynb`: Notebook with initial data exploration, cleaning, and descriptive analysis

## Methodology
The project uses a **causal inference framework** (e.g., before-after comparison or Difference-in-Differences) to estimate how congestion pricing affects:

- Trip volumes  
- Entry/exit patterns into the CPZ  
- Spatial mobility trends  

## Policy Context
- Standard vehicles: ~$9 daily toll  
- Taxis: $0.75 per trip  
- High-volume for-hire vehicles: $1.50 per trip  

The policy aims to:
- Reduce congestion  
- Improve mobility  
- Enhance air quality  

## How to Use
1. Load data from the `data/` folder (Parquet + CSV)  
2. Run the EDA notebook in `eda/`  
3. Extend analysis with causal models or visualizations

## License
This project is open for public use, and anyone is encouraged to utilize, modify, and distribute it to advance the public good. We ask that you acknowledge the original authorship and respect the ethical use of the project. While the project is provided as-is without warranty, we encourage its use for positive societal impact, including in areas of social research, education, and community development. Please ensure your use aligns with ethical standards and contributes to meaningful outcomes.
