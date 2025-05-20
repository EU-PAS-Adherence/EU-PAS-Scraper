# EU PAS Scraper
![GitHub top language](https://img.shields.io/github/languages/top/EU-PAS-Adherence/EU-PAS-Scraper?style=flat) ![GitHub License](https://img.shields.io/github/license/EU-PAS-Adherence/EU-PAS-Scraper?style=flat)

This repository contains the scripts used to scrape and analyse the metadata of the [HMA-EMA RWD Studies Catalogue](https://catalogues.ema.europa.eu/) (short: Catalogue) on **21 February 2024**, with the aim to evaluate the adherence of post-authorisation studies (PAS) with legislation and recommendations to make public protocols and results.

The Catalogue is a registry for non-interventional PAS managed by the European Medicines Agency (EMA). Non-interventional PAS included in a risk management plan (RMP) category 1 or 2 are **required** by law to register the protocol and results in this registry. Other non-interventional PAS are only **recommended** to register their protocol and results in this registry. 

> [!IMPORTANT]
> The scraping script will **NOT** work on the current version of the Catalogue without modifications. However, you can extract almost all data using the **CSV export function** of the Catalogue website. The exceptions are the document URLs and fields with multiple values, which you may want to extract using a different separator (the default is a comma) to handle values containing commas correctly. Otherwise, the values will be split into multiple parts.

> [!NOTE]
> The project relies on the scraping framework [Scrapy](https://github.com/scrapy/scrapy/) written for Python. There are many [settings](/eupas/settings.py) which can be modified to customize the behavior of the scripts. The scripts were originally written to scrape the [EU PAS Register](https://encepp.europa.eu/encepp/studiesDatabase.jsp). The EU PAS Register was migrated to the [Catalogue](https://catalogues.ema.europa.eu/) on **February 15, 2024**. The scripts were then adapted to scrape the Catalogue. All files containing the phrase "eupas" were used to scrape the EU PAS Register and are therefore deprecated.
>
> The scraper was originally supposed to run periodically to extract datasets for a live website. However, this was removed as there were too many issues requiring manual checks and mappings. We created a static website instead with the dataset from 21 February 2024, and the code for this is included in another [repository](https://github.com/EU-PAS-Adherence/EU-PAS-Adherence-Website).
>
> Some remnants of the original approach remain, including the JSON schema checks to detect false items, the comparisons with older runs, other checks at the Scrapy level, Python tests and coverage (not finished), the Dockerfile and the GitLab CI/CD file, to name a few.

## Usage
If you want to run this project on your own machine, follow these steps:
* Clone the repository
* Install Python 3.8 or newer
* Setup an environment with `venv` or `conda`
* Install the required Python packages 
  - Take a look [here](https://docs.scrapy.org/en/latest/intro/install.html) to see the requirements for Scrapy and follow the instructions
  - If you have everything ready, run the following command in the project folder
    ```sh
    python pip install -r requirements.txt
    ```
* Finally run the following command
  ```sh
  scrapy ema_rwd [options]
  ``` 
  in the project directory
  - This will scrape all studies in the catalogue
  - You can use the option `--debug` to get more logs
  - You can filter the study cohort with the option `-F filter_level`, where `filter_level` can be:
    - a string like "*rmp2*", "*n/a*" or "*non_eu_rmp*" to filter by **RMP category**
    - a id like "*eupas1111*", "*Eupas1111*" or "*1111*" to filter for all studies with an **EU PAS Register number** starting with *1111* (e.g 1111, 11110, 111123, etc.)
  - You can use the option `--download-pdf` to additionaly scrape each study as a `.pdf` file
  - You can use the option `--download-protocols-results` to additionaly scrape the latest protocols and results for each study as a `.pdf` file
  - You can also use all of the default scrapy options. Use `-h` to see all available options.
* The data and reports are stored in a folder named `output` in the project folder
* There are many [settings](/eupas/settings.py) which can be changed to customize the behavior of the script

## Output
Once run the script will generate a folder based on the current *UTC-Time* with the scraped data.

The data is provided in the `.csv`, `.db`,  `.json`, `.xlxs` and `.xml` format. 

## Testing/Development
If you want to test or further develop this project, follow these additional steps:
* Run the following command in the project folder
  ```sh
  pip install -r requirements.testing.txt
  ```
* Install the package in editable mode
  ```sh
  pip install -e .
  ```
* Run the tests with `tox` or `pytest`

## Additional Scripts
If you want to run any other command of this project, follow this additional step:
* Run the following command in the project folder
  ```sh
  pip install -r requirements.additional.txt
  ```
### Cluster
You can cluster the unique values of specified column in the scraped data:
* Run the following command
  ```sh
  scrapy cluster fieldnames [options]
  ``` 
  in the project directory
  - This will group all specified fieldnames
  - You have to specify the input json with the `-i` option
  - You have to specify the output directory with the `-o` option
  - You have to specify a similarity cutoff in `[0,1]` with the `-c` option
* The generated `.xlxs` file is stored in the specified output folder.

### Patch
You can patch (add extra columns) the scraped data:
* Run the following command
  ```sh
  scrapy patch patch_name [options]
  ``` 
  in the project directory. Based on `patch_name` this will do the following:
  - `match`: This will match the sponsor names if provided with a matching spreadsheet
  - `state`: correct the state variable
  - `cancel`: This will detect cancelled studies
* The patched data is stored in the specified output folder.

### Statistic
You can generate statistics (for paper and website) with patched scraped data (see other [repository](https://github.com/EU-PAS-Adherence/EU-PAS-Data-and-Notebooks) with notebooks for additional steps/analysis):
* Run the following command
  ```sh
  scrapy ema_rwd_statistic -D compare_date [options]
  ``` 
  in the project directory
  - You have to specify the extraction date as `compare_date`
  - This will generate statistics, plots, tables and logistic regression models
* The files are stored in the specified output folder.
