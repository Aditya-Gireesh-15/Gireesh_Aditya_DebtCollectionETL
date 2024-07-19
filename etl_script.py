#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime as datetime
from datetime import date
from ast import literal_eval
import numpy as np
import ast 
import re


# # Data Extraction

# In[2]:


Data = pd.read_csv("5k_borrowers_data.csv")
Data.info()   #There are no null values.


# In[3]:


Data.head(5)


# In[4]:


#The date format should be converted to dd/mm/yy format so that we can able to access them in future.

Data['Repayment History'] .head(10)


# # Data Transformation

# In[5]:


#Replacing the empty array"[]" with assumed date 28/2/3252, to just make the data loadable in the database.
#There are total of 775 records where the repayment history is missing. With this assumed  date we can infer that they havent paid yet.

def replace_empty_with_default(s):
    if s.strip() == "[]":
        return "[{'Payment Date': datetime.date(3252, 2, 28), 'Payment Mode': 'None'}]"
    else:
        return s

# Apply the function to 'Repayment History' column
Data['Repayment History'] = Data['Repayment History'].apply(replace_empty_with_default)


# In[6]:


#It is found that the data were in string format, so to process the data it should be in dictionary form
#So at the first step here we are striping the square brackets from the data

import ast
def process_string(s):
    # Remove '[' and ']' and convert to list   
    s = s.strip('[]')
      
    # Use eval to convert string to dictionary
    return eval(s)


# Apply the function to 'Repayment History' column
Data['Repayment History'] = Data['Repayment History'].apply(process_string)
Data['Repayment History']


# In[7]:


# Here we are adding the sqare bracket to enclose them into list, Now we have converted it from string to proper dictionary.
Data['Repayment History'] = Data['Repayment History'].apply(lambda x: [x])
Data['Repayment History']


# In[8]:


# Here we are defining a Function to convert date to dd/mm/yy format for all the rows in 'Repayment History' column where there is only single dictionary.
#In later part of the code we will also take care of the data which has multiple dictionaries.

def format_payment_date(payment_dict):
    if 'Payment Date' in payment_dict and isinstance(payment_dict['Payment Date'], datetime.date):
        payment_dict['Payment Date'] = payment_dict['Payment Date'].strftime('%d/%m/%y')
    return payment_dict


Data['Repayment History'] = Data['Repayment History'].apply(lambda payments: [format_payment_date(payment) for payment in payments])

Data['Repayment History']


# In[9]:


#This part of code is to convert dictionary to string so that it can be loaded in the database.
#sqllite database is not accepting dicionary, it is accepting only data in string format for 'Repayment History' column

import json

Data['Repayment History'] = Data['Repayment History'].apply(lambda x: json.dumps(x, default=str))


# In[10]:


#The rows with single dictionary has date in dd/mm/yy format but rows with multiple dictionary is not in dd/mm/yy format,
#so in this section we are converting all the rows to dd/mm/yy format



from datetime import datetime
def fix_date_strings(rep_hist_str):

    rep_hist_str = re.sub(r'(?<=: )(\d{2}/\d{2}/\d{2})(?=[,}])', r'"\1"', rep_hist_str)
    rep_hist_str = re.sub(r'(?<=: )(\d{4}-\d{2}-\d{2})(?=[,}])', r'"\1"', rep_hist_str)
    return rep_hist_str

def convert_date_format(rep_hist_str):
    rep_hist_str = fix_date_strings(rep_hist_str)
    rep_hist_list = ast.literal_eval(rep_hist_str)
    new_rep_hist_list = []
    for entry in rep_hist_list:
        if isinstance(entry, list):
            new_entry = []
            for sub_entry in entry:
                date_str = sub_entry['Payment Date']
                if '-' in date_str:
                    new_date_str = datetime.strptime(date_str, '%Y-%m-%d').strftime('%d/%m/%y')
                else:
                    new_date_str = datetime.strptime(date_str, '%d/%m/%y').strftime('%d/%m/%y')
                new_entry.append({'Payment Date': new_date_str, 'Payment Mode': sub_entry['Payment Mode']})
            new_rep_hist_list.append(new_entry)
        else:
            date_str = entry['Payment Date']
            new_date_str = datetime.strptime(date_str, '%d/%m/%y').strftime('%d/%m/%y')
            new_rep_hist_list.append({'Payment Date': new_date_str, 'Payment Mode': entry['Payment Mode']})
    return str(new_rep_hist_list)

Data['Repayment History'] = Data['Repayment History'].apply(convert_date_format)
Data['Repayment History'] 


# In[11]:


import re

#This code is to count the number of times the EMI has been paid

def count_payment_dates(rep_hist_str):
    # Add quotes around unquoted date strings
    rep_hist_str = re.sub(r'(?<=: )(\d{2}/\d{2}/\d{2})(?=[,}])', r'"\1"', rep_hist_str)
    # Parse the string representation to a list
    rep_hist_list = ast.literal_eval(rep_hist_str)
    
    # Flatten the list if it contains nested lists
    if isinstance(rep_hist_list, list) and all(isinstance(i, list) for i in rep_hist_list):
        rep_hist_list = [item for sublist in rep_hist_list for item in sublist]

    # Count the number of 'Payment Date' entries
    return sum('Payment Date' in d for d in rep_hist_list)

Data['Number_of_EMI_paid'] = Data['Repayment History'].apply(count_payment_dates)

Data['Monthly Interest Rate'] = Data['Interest Rate'] / 12

#Here we are calculating outstanding amount and having it in a separate column

Data['Outstanding_Balance'] = Data.apply(
    lambda row: row['Loan Amount'] * (
        ((1 + row['Monthly Interest Rate']) ** row['Loan Term'] - (1 + row['Monthly Interest Rate']) ** row['Number_of_EMI_paid']) /
        ((1 + row['Monthly Interest Rate']) ** row['Loan Term'] - 1)
    ),
    axis=1
)

Data.info()


# Formula to calculate Outstanding amount
# 
# ![image.png](attachment:image.png)
# 
# ![image-4.png](attachment:image-4.png)

# # Data Loading to SQLlite database

# In[12]:


#Here we are using sqllite as the source of database.
#Required libraries are loaded and database name is '5K_borrowers' and the table name is 'borrowers'

get_ipython().system('pip install PyMySQL')
get_ipython().system('pip install sqlalchemy')
import sqlalchemy
import sqlite3
from sqlalchemy import create_engine

engine = sqlalchemy.create_engine('sqlite:///5K_borrowers.db')
Data.to_sql('borrowers', con=engine, index=False, if_exists='replace')


# # Querying required data
# 
# 

# # a. What is the average loan amount for borrowers who are more than 5 days past due?

# In[13]:


AVG_Loan_amount_5days_past_due_query = """
SELECT AVG("Loan Amount") AS average_loan_amount
FROM borrowers
WHERE "Days Left to Pay Current EMI" <= -5
"""


AVG_Loan_amount_5days_past_due = pd.read_sql(AVG_Loan_amount_5days_past_due_query, con=engine)
AVG_Loan_amount_5days_past_due


# Since the 'Days Left to Pay Current EMI' column is in positive value we are getting no data for average loan amount for more than 5days past due

# # b. Who are the top 10 borrowers with the highest outstanding balance?

# These are the 10 borrowers with highest outstanding history.Vaibhav Sharaf stands top

# In[14]:


TOP_10_Outstanding_balance_query = "SELECT Name FROM borrowers ORDER BY Outstanding_Balance DESC Limit 10"

TOP_10_Outstanding_balance = pd.read_sql(TOP_10_Outstanding_balance_query, con=engine)
TOP_10_Outstanding_balance


# # c  List of all borrowers with good repayment history

# The assumption here I have done is I have considered people who has dome repayment twice in good condition.
# 
# 3455 borrowers has good repayment history

# In[15]:



names_with_Good_repayment_history_query = "SELECT Name FROM borrowers where Number_of_EMI_paid >= 2 ORDER BY Number_of_EMI_paid DESC "


names_with_Good_repayment_history = pd.read_sql(names_with_Good_repayment_history_query, con=engine)
names_with_Good_repayment_history


# # d  Brief analysis wrt loan type

# By the Below query results we can infer that 
# the most borrowers opted for Auto Loan are malayalies,
# the most borrowers opted for Personal Loan are Bengalies
# and most borrowers opted for Home Loan are Telugu speakers.
# 
# And the suspicious thing ithat we can infer after the analysis is, 305 borrowers have opted for home loan for medical emergency purpose which we shall introspect.

# In[16]:


query_Auto_Loan = """
SELECT "Language Preference", COUNT(*) AS count
FROM borrowers 
WHERE "Loan Type" = 'Auto Loan'
GROUP BY "Language Preference"
ORDER BY count
DESC
"""


Auto_loan_query = pd.read_sql(query_Auto_Loan, con=engine)
Auto_loan_query



# In[17]:


query_personalloan = """
SELECT "Language Preference", COUNT(*) AS count
FROM borrowers 
WHERE "Loan Type" = 'Personal Loan'
GROUP BY "Language Preference"
ORDER BY count
DESC
"""
personal_loan_query = pd.read_sql(query_personalloan, con=engine)
personal_loan_query


# In[18]:


query_home_loan = """
SELECT "Language Preference", COUNT(*) AS count
FROM borrowers 
WHERE "Loan Type" = 'Home Loan'
GROUP BY "Language Preference"
ORDER BY count
DESC
"""
home_loan_query = pd.read_sql(query_home_loan, con=engine)
home_loan_query


# In[19]:


query_home_loan_medical = """
SELECT Name
FROM borrowers 
WHERE "Loan Type" = 'Home Loan'
AND "Loan Purpose" = 'Medical Emergency'
ORDER BY "Loan Amount"
DESC
"""
home_loan_medical_query = pd.read_sql(query_home_loan_medical, con=engine)
home_loan_medical_query


# # Saving sql files

# In[20]:


import os
print(os.getcwd())


# In[21]:



# Function to save a query to a file
def save_query_to_file(query, filename):
    with open(filename, 'w') as file:
        file.write(query)

# Save queries to .sql files
save_query_to_file(AVG_Loan_amount_5days_past_due_query, 'AVG_Loan_amount_5days_past_due_query.sql')
save_query_to_file(TOP_10_Outstanding_balance_query, 'TOP_10_Outstanding_balance_query.sql')
save_query_to_file(names_with_Good_repayment_history_query, 'names_with_Good_repayment_history_query.sql')

save_query_to_file(query_personalloan, 'personal_loan_query.sql')
save_query_to_file(query_home_loan, 'home_loan_query.sql')
save_query_to_file(query_Auto_Loan, 'auto_loan_query.sql')


# In[ ]:




