import sys, os
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
#os.chdir(os.path.dirname(os.path.realpath(__file__)))
os.chdir('/it/makozlow/python_development/ad_hoc/nauka')

'''
Data reading to pandas' dataframes:
    *target
    *customer
    *payments
    *orders
    *transactions
'''
data_dict = {}
with open('customers.txt', 'r') as f:
    for line in f.readlines():
        rec = json.loads(line)
        for key in rec:
            try:
                data_dict[key].append(rec[key])
            except KeyError:
                data_dict[key]=[]
                data_dict[key].append(rec[key])

target = pd.DataFrame(data_dict['fraudulent'])
target.columns = ['fraudulent']
target['CUSTOMER_ID'] = target.index

customer = pd.DataFrame(data_dict['customer'])
customer['CUSTOMER_ID'] = customer.index

orders = []
transactions = []
payments = []
for i in range(target.shape[0]):
    t_orders = pd.DataFrame(data_dict['orders'][i])
    t_orders['CUSTOMER_ID']=i
    orders.append(t_orders)
    
    t_transactions = pd.DataFrame(data_dict['transactions'][i])
    t_transactions['CUSTOMER_ID']=i
    transactions.append(t_transactions)
    
    t_payments = pd.DataFrame(data_dict['paymentMethods'][i])
    t_payments['CUSTOMER_ID']=i
    payments.append(t_payments)

orders = pd.concat(orders, sort = True)
payments = pd.concat(payments, sort = True)
transactions = pd.concat(transactions, sort = True)
###DATA READING DONE


'''
DATA PREPARATION:
FIRSTLY REMOVE DUPLICATED CUSTOMERS ON EMAILS - PHONES OK - coming back to EXPLORATORY ANALYSIS, mark duplicated mails
CUSTOMER_ID ==7(johnlowery@gmail.com) to be excluded from data - outlier - possible test user 

Based on exploratory analysis data should have below listed fields:
    *CLIENT_ID
    *NO_OF_ORDERS
    *NO_OF_FAILED_ORDERS
    *AVG_ORDER_AMOUNT (clean outliers)
    *NO_OF_ORDERS_ABOVE_AVG_AMOUNT
    *NO_OF_ORDERS_ABOVE_75_percentile_AMOUNT
    *NO_OF_ORDERS_DIFF_ADDR
    *PERC_OF_ORDERS_DIFF_ADDR
    *NO_OF_FAILED_TRANSACTIONS
    *PRESENT_OF_SUSPECTED_ISSUER (NORMAL AND CLEANED)
    *ISSUERS_INDICATORS
    *PAYMENT METHODS - indicators
    *NO_OF_PAYMENT_METHODS (clean outliers)
'''


##DUPLICATES
customer['customerEmail'] = customer['customerEmail'].str.upper()
duplicated_mail = customer.groupby('customerEmail').customerEmail.count()
duplicated_mail = list(duplicated_mail[duplicated_mail>1].index)

duplicates = customer[customer['customerEmail'].isin(duplicated_mail)]

for d in duplicated_mail:
    ids_to_update = list(duplicates[duplicates.customerEmail==d]['CUSTOMER_ID'])
    update_id = ids_to_update[0]
    target.loc[target.CUSTOMER_ID.isin(ids_to_update), 'CUSTOMER_ID']=update_id
    customer.loc[customer.CUSTOMER_ID.isin(ids_to_update), 'CUSTOMER_ID']=update_id
    payments.loc[payments.CUSTOMER_ID.isin(ids_to_update), 'CUSTOMER_ID']=update_id
    orders.loc[orders.CUSTOMER_ID.isin(ids_to_update), 'CUSTOMER_ID']=update_id
    transactions.loc[transactions.CUSTOMER_ID.isin(ids_to_update), 'CUSTOMER_ID']=update_id
    
data = customer[['CUSTOMER_ID']].drop_duplicates('CUSTOMER_ID')
data['DUPLICATED']=0
data.loc[data.CUSTOMER_ID.isin(duplicates.CUSTOMER_ID), 'DUPLICATED']=1

#ORDERS DATA PREP AND CLEAN
orders.loc[orders.orderAmount>100, 'orderAmount']=100
orders['AVG_ORDER'] = orders.orderAmount.mean()
orders['PERC_75'] = orders.orderAmount.quantile(.75)
orders = orders.merge(customer[['CUSTOMER_ID', 'customerBillingAddress']], on = 'CUSTOMER_ID', how = 'left')
orders['DIFF_ADDR']=0
orders.loc[orders.orderShippingAddress != orders.customerBillingAddress, 'DIFF_ADDR']=1

temp = orders.groupby('CUSTOMER_ID').orderId.count().reset_index()
temp = temp.rename(columns = {"orderId":"NO_OF_ORDERS"})
data = data.merge(temp, how = 'left')

temp = orders[orders.orderState=='failed'].groupby('CUSTOMER_ID').orderId.count().reset_index()
temp = temp.rename(columns = {"orderId":"NO_OF_FAILED_ORDERS"})
data = data.merge(temp, how = 'left')
data['NO_OF_FAILED_ORDERS'] = data.NO_OF_FAILED_ORDERS.fillna(0)

temp = orders.groupby('CUSTOMER_ID').orderAmount.mean().reset_index()
temp = temp.rename(columns = {"orderAmount":"AVG_ORDER_AMNT"})
data = data.merge(temp, how = 'left')

temp = orders
temp['ORDER_AMNT_MORE_50_cnt'] = 0
temp['ORDER_AMNT_MORE_75_cnt'] = 0
temp.loc[temp.orderAmount>temp.AVG_ORDER, 'ORDER_AMNT_MORE_50_cnt']=1
temp.loc[temp.orderAmount>temp.PERC_75, 'ORDER_AMNT_MORE_75_cnt']=1
temp1 = temp.groupby('CUSTOMER_ID').ORDER_AMNT_MORE_50_cnt.sum().reset_index()
temp2 = temp.groupby('CUSTOMER_ID').ORDER_AMNT_MORE_75_cnt.sum().reset_index()
data = data.merge(temp1, how = 'left')
data = data.merge(temp2, how = 'left')
data['ORDER_AMNT_MORE_50_cnt'] = data.NO_OF_FAILED_ORDERS.fillna(0)
data['ORDER_AMNT_MORE_75_cnt'] = data.NO_OF_FAILED_ORDERS.fillna(0)

temp = orders
temp = temp.groupby('CUSTOMER_ID').DIFF_ADDR.agg(['max', 'sum']).reset_index()
temp = temp.rename(columns = {'max':'ORDERS_DIFF_ADD_ind',
             'sum': 'ORDERS_DIFF_ADD_cnt'})
data = data.merge(temp, how = 'left')
data['ORDERS_DIFF_ADD_ind'] = data.NO_OF_FAILED_ORDERS.fillna(0)
data['ORDERS_DIFF_ADD_cnt'] = data.NO_OF_FAILED_ORDERS.fillna(0)
data['ORDERS_DIFF_ADD_share'] = (data.ORDERS_DIFF_ADD_cnt)/(data.NO_OF_ORDERS)

#TRANSACTIONS DATA PREP AND CLEAN
transactions['transactionFailed'] = transactions.transactionFailed.astype('str')
temp = transactions[transactions.transactionFailed=='True'].groupby('CUSTOMER_ID').transactionId.count().reset_index()
temp = temp.rename(columns = {'transactionId':'FAILED_TRAN_cnt'})
data = data.merge(temp, how = 'left')
data['FAILED_TRAN_cnt'] = data.NO_OF_FAILED_ORDERS.fillna(0)
#PAYMENTS METHODS
issuers = payments.merge(target, on = 'CUSTOMER_ID', how = 'left')

bad_issuers = issuers[issuers.fraudulent]['paymentMethodIssuer'].reset_index().drop_duplicates('paymentMethodIssuer')
good_issuers = issuers[~(issuers.fraudulent)]['paymentMethodIssuer'].reset_index().drop_duplicates('paymentMethodIssuer')

susp_issuers = bad_issuers[~(bad_issuers.paymentMethodIssuer.isin(good_issuers.paymentMethodIssuer))]

payments['SUSP_ISSUER'] = 0
payments['SUSP_ISSUER_clean'] = 0

payments.loc[payments.paymentMethodIssuer.isin(bad_issuers.paymentMethodIssuer), 'SUSP_ISSUER']=1
cond1 = payments.paymentMethodIssuer.isin(susp_issuers.paymentMethodIssuer)
cond2 = payments.paymentMethodIssuer.str.len()<=2
payments.loc[(cond1) | (cond2), 'SUSP_ISSUER_clean']=1

temp = payments.groupby('CUSTOMER_ID').SUSP_ISSUER.agg(['sum', 'max']).reset_index()
temp = temp.rename(columns = {'sum':'SUSP_ISSUER_cnt', 'max':'SUSP_ISSUER_ind'})
data = data.merge(temp, how = 'left')
data['SUSP_ISSUER_cnt'] = data.NO_OF_FAILED_ORDERS.fillna(0)
data['SUSP_ISSUER_ind'] = data.NO_OF_FAILED_ORDERS.fillna(0)

temp = payments.groupby('CUSTOMER_ID').SUSP_ISSUER_clean.agg(['sum', 'max']).reset_index()
temp = temp.rename(columns = {'sum':'SUSP_ISSUER_CLEAN_cnt', 'max':'SUSP_ISSUER_CLEAN_ind'})
data = data.merge(temp, how = 'left')
data['SUSP_ISSUER_CLEAN_cnt'] = data.NO_OF_FAILED_ORDERS.fillna(0)
data['SUSP_ISSUER_CLEAN_ind'] = data.NO_OF_FAILED_ORDERS.fillna(0)

temp = payments.groupby('CUSTOMER_ID').paymentMethodId.count().reset_index()
temp = temp.rename(columns = {'paymentMethodId':'PAY_METH_cnt'})
data = data.merge(temp, how = 'left')
data['PAY_METH_cnt'] = data.NO_OF_FAILED_ORDERS.fillna(0)

methods_ind = payments.groupby(['CUSTOMER_ID', 'paymentMethodType'])['paymentMethodType'].count().unstack(fill_value=0).reset_index()
issuers_ind = payments.groupby(['CUSTOMER_ID', 'paymentMethodIssuer'])['paymentMethodIssuer'].count().unstack(fill_value=0).reset_index()

data = data.merge(methods_ind, how = 'left')
data = data.merge(issuers_ind, how = 'left')

###DATA PREPARATION DONE
data.to_parquet('output/data_prepared.parquet')

