# NBA Sports Data Pipeline

A beginner data engineering project that I did to practice and get familiar with.
Itcollects, cleans,  analyzes and stores NBA player statistics using Python.

## What this project does
- Loads NBA player stats from a CSV dataset
- Cleans and transforms the data using pandas
- Stores clean data in a SQLite database
- Generates an automated report with top scorers and player tiers
- Visualizes insights with matplotlib and seaborn

## Tech stack
- Python
- pandas
- SQLite
- matplotlib / seaborn
- Jupyter Notebook

## Project structure
- `pipeline.py` — automated ETL pipeline (Extract, Transform, Load)
- `phase1.ipynb` — data collection and first look
- `phase2.ipynb` — data cleaning and exploration
- `phase3.ipynb` — data visualization
- `phase4.ipynb` — SQL queries with SQLite

## How to run
1. Download the NBA dataset from Kaggle
2. Install dependencies: `pip install pandas matplotlib seaborn`
3. Run the pipeline in the terminal: `python pipeline.py`

## Key findings
- Top scorer across all seasons
- Most well-rounded player
- Dataset covers _ seasons and _ unique players