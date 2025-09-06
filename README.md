this project generates google trends queries for specific country-language-topic combinations, downloads the raw data, and then post-processes that data with ratio-based linking. to run the project, follow the following steps:

This fork specializes the program to work with divisions rather than just countries. Specifically, it allows us to work with data for specific divisions within Bangladesh rather than the country as one. 

## 1. generate queries

1. run `build_queries.py`:

    ```bash
    python build_queries.py
    ```

    - this script creates a root data directory named `data/`.
    - inside `data/`, it creates a subdirectory for each country-language pair (e.g., `cuba_spanish`).
    - inside each country-language directory, there will be a `queries/` subdirectory containing one csv file per topic (e.g., `environment`, `politics`, etc.). each csv holds 4-keyword group queries plus the anchor keyword for google trends.


## 2. manual scraping of raw data

1. for each country-language and topic, **manually** download the raw csv files from google trends using the query urls generated in the previous step.
2. in the corresponding `data/<country_language>/` directory, **create a new folder named after the topic**.
3. place all downloaded csv files (`multitimeline.csv`, `multitimeline (1).csv`, etc.) in that topic folder.

**after this step, your data directory structure should look like this:**

```
data/
├── cuba_spanish/
│   ├── queries/
│   │   ├── environment_cuba_spanish_queries.csv
│   │   ├── politics_cuba_spanish_queries.csv
│   │   └── ...
│   ├── environment/
│   │   ├── multitimeline.csv
│   │   ├── multitimeline (1).csv
│   │   └── ...
│   └── politics/
│       ├── multitimeline.csv
│       ├── multitimeline (1).csv
│       └── ...
└── ...
```


## 3. process the raw files

1. once all raw data files for each topic are in place, run:

    ```bash
    python process_raw.py
    ```

    - this script traverses each country-language directory and each topic subdirectory to perform the ratio-based linking. 
    - it produces two output csv files per topic inside the same country-language directory:
      - **`<topic>_<country>_<language>_raw.csv`**:  
         a simple concatenation of all time-series data (outer join on the `month` column) without any scaling.
      - **`<topic>_<country>_<language>_adjusted.csv`**:  
         the ratio-linked data, in which each keyword’s relative values are scaled to a consistent baseline using the anchor keyword. 



### notes

- file naming is a bit finicky, but it has to follow the patterns outlined above for the scripts to work correctly.

---
