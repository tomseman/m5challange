  _____ _       _   _      
 |___  | |_   _| |_(_)_  __
    / /| | | | | __| \ \/ /
   / / | | |_| | |_| |>  < 
  /_/  |_|\__, |\__|_/_/\_\
          |___/            

Application task for an employment at 7lytix (www.7lytix.com) as a data engineer:

#######################################################################################
Task: Short information:
-download a dataset from kaggel (m5 forecasting - accuracy: https://www.kaggle.com/c/m5-forecasting-accuracy/data?select=calendar.cs: https://www.kaggle.com/c/m5-forecasting-accuracy/data?select=calendar.csv) 
-prepare a standartized form and design to process similar data in order to make it easy-to-use for an data analyst

#######################################################################################
Files and function: 

-data_description.ipynb: a Jupyter notebook to get some insight into the datasets

-main.py: script to create a data model with postgresql and upload the dataset:
    open a terminal and execute:
    python main.py <calendarData.csv> <priceData.csv> <salesData.csv> <connectionString>

    e.g. in terminal: python main.py /home/tom/projects/m5challange_prod/data/tmp/calendar.csv \
                                  /home/tom/projects/m5challange_prod/data/tmp/sell_prices.csv \
                                  /home/tom/projects/m5challange_prod/data/tmp/sales_train_evaluation.csv \
                                   postgresql://tom:1a5d9g@localhost:5432/lnz_sale
    (no arguments will use the default subdirectory /data )

-upload_function: outsourced functions used by "main.py"

-information: dataset from kaggle is not part of this upload!
    - be sure to have 3 datasets in subdirectory /data before execution:
      -> calendar.csv
      -> sell_prices.csv
      -> sales_train_evaluation.csv

-the final dataset is available with: SELECT * FROM walmart.v_sales;