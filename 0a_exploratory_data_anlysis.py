import sys, os
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
#os.chdir(os.path.dirname(os.path.realpath(__file__)))
os.chdir('/it/makozlow/python_development/ad_hoc/nauka')
'''
EXPLORATORY DATA ANALYSIS QUESTIONS and ANSWERS:
ALL CHARTS ARE in 'figures' dict
   *ORDERS
    --orders count (groupby order_state) vs target
        only fraudulents have orders above 5 - ADD ORDERS_NO to final data
        no diff between order_state in targets groups - COULD ADD no of FAILED to model
    --orders amount vs target
        outliers for amount
        frauders order amounts are higher - ADD AV ORDER AMOUNT to FAINAL final_data,
                                            ADD NO OF HIGH ORDERS to final_data
    --order shipping differnt than customer's billing address
        looks like possible significant impact - ADD DIFF ADDRESS to final_data
                                                ADD NO OF ORDERS WITH DIFF ADDRESS to final_data
                                                ADD PERCENTAGE OF ORDERS WITH DIFF ADDRESS to final_data

    *TRANSACTIONS
    -- transaction amounts
        will be corelated with order amount, check which one is better
    --transaction failed vs target
        no diff between transaction_status in targets groups - COULD ADD no of FAILED transacions to model
    
    *PAYMENTS METHOD
    -- paymentMethodIssuer vs target
        there are issuers with only frauds
        these issuers often has 'strange' name (check data)
        ADD PAYMENT ISSUER to final_data
        ADD IND_OF_FRAUDULENT_ISSUAER to final_data
        CLEAN FRAUDULENT issuers by substracting issuers which are on nonfrodulents customers
    -- paymentMethodType vs target
        there are metods which are more trustable than others
        ADD PAYMENT METHOD to final_data
        ADD TRUST PAYMENT METHOD to final data
        mayby corelated with issuer - check combination
    -- combination of paymentMethodType and paymentMethodIssuer
        different methods on the same issuaer have different impact on targer
        ADD COMBINED VARIABLE
    -- number of payments method vs target
        nonFraud has no more (majority one) than 4 payments methods
        outliers
        ADD VARIABLE to final_data
    *GENERAL
    -- attempts of use different payment methods with many fails - it will be corelated with no of payments methods
    -- bitcoin?? - data looks that it is rather 'safe' payment
    -- trusted groups of payment methods (with more unfraud payments)
    -- fake/foreign phones numbers - can't see any impact
'''
orders = pd.read_parquet('output/orders.paruqet')
payments = pd.read_parquet('output/payments.paruqet')
transactions = pd.read_parquet('output/transactions.paruqet')
customer = pd.read_parquet('output/customer.paruqet')
target = pd.read_parquet('output/target.paruqet')
figures = {}
######ORDERS
title = 'Orders count vs target'
orders_a =orders.merge(target)
hist = orders_a.groupby(['CUSTOMER_ID', 'fraudulent'])['orderId'].nunique().reset_index()
hist = hist.rename(columns={'orderId':'ORDERS_NO'})
fig = go.Figure()
fig.add_trace(go.Histogram(x = hist[hist.fraudulent].ORDERS_NO,
                           name = 'Fraudulent')
              )
fig.add_trace(go.Histogram(x = list(hist[~hist.fraudulent].ORDERS_NO),
                           name = 'nonFraudulent')
              )
fig.update_layout(xaxis_title="Orders count",
                  yaxis_title="Customers count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
##fig.show()

title = 'Orders state percentage vs target'
hist = orders_a.groupby(['CUSTOMER_ID', 'fraudulent', 'orderState'])['orderId'].nunique().reset_index()
hist.columns = ['ORDERS_NO' if x =='orderId' else x for x in hist.columns]
hist = hist.groupby(['orderState', 'fraudulent']).agg({'ORDERS_NO':'sum'}).reset_index()
t=hist.groupby('fraudulent').ORDERS_NO.sum().reset_index()
t= t.rename(columns={'ORDERS_NO':'TOTAL_ORDERS'})
hist = hist.merge(t, on = 'fraudulent')
hist['share'] = hist.ORDERS_NO/hist.TOTAL_ORDERS

fig = go.Figure()
fig.add_trace(go.Bar(x = hist[hist.fraudulent].orderState,
                     y = hist[hist.fraudulent].share,
                    name = 'Fraudulent')
              )
fig.add_trace(go.Bar(x = hist[~hist.fraudulent].orderState,
                     y = hist[~hist.fraudulent].share,
                    name = 'nonFraudulent'))
fig.update_layout(xaxis_title="Order status",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

title = 'Orders amount vs target'
fig = go.Figure()
fig.add_trace(go.Histogram(x = orders_a[(orders_a.fraudulent) & (orders_a.orderAmount<200)].orderAmount,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = orders_a[(~orders_a.fraudulent) & (orders_a.orderAmount<200)].orderAmount,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Order amount",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

title = 'Difference in shipping and billing addresses'
orders_a = orders_a.merge(customer[['CUSTOMER_ID', 'customerBillingAddress']], on = 'CUSTOMER_ID')
orders_a['ADDRESS_DIFF'] = 0
cond = orders_a['customerBillingAddress'] != orders_a['orderShippingAddress']
orders_a.loc[cond, 'ADDRESS_DIFF']=1

fig = go.Figure()
fig.add_trace(go.Histogram(x = orders_a[(orders_a.fraudulent)].ADDRESS_DIFF,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = orders_a[(~orders_a.fraudulent)].ADDRESS_DIFF,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Difference in addresses",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

######TRANSACTIONS
title = 'Transaction statuses vs target'
transactions_a = transactions
transactions_a = transactions_a.merge(target[['CUSTOMER_ID', 'fraudulent']])

fig = go.Figure()
fig.add_trace(go.Histogram(x = transactions_a[(transactions_a.fraudulent)].transactionFailed,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = transactions_a[~(transactions_a.fraudulent)].transactionFailed,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Transaction status",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

######PAYMENT METHODS
payments_a = payments.merge(target[['CUSTOMER_ID', 'fraudulent']])

title = 'Payment issuers vs target'
fig = go.Figure()
fig.add_trace(go.Histogram(x = payments_a[(payments_a.fraudulent)].paymentMethodIssuer,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = payments_a[~(payments_a.fraudulent)].paymentMethodIssuer,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Issuer",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

title = 'Payment method vs target'
fig = go.Figure()
fig.add_trace(go.Histogram(x = payments_a[(payments_a.fraudulent)].paymentMethodType,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = payments_a[~(payments_a.fraudulent)].paymentMethodType,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Payment method",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

title = 'Different payments methods of same issuer vs target'
issuers = list(payments_a['paymentMethodIssuer'].unique())
fig = make_subplots(cols=1, rows= len(issuers), subplot_titles=issuers)
for i, iss in enumerate(issuers):
    cond = payments_a['paymentMethodIssuer']==iss
    fig.add_trace(go.Histogram(x = payments_a[(payments_a.fraudulent) & (cond)].paymentMethodType),
                  col=1, row=i+1)
    fig.add_trace(go.Histogram(x = payments_a[~(payments_a.fraudulent) & (cond)].paymentMethodType),
                  col=1, row=i+1)
    
fig.update_layout(height=len(issuers)*200, width=1000,
                  title_text="Different paymnets methods on one issuer")
figures[title] = fig
#fig.show()

title = 'No of customer payment methods vs target'
no_p_methods = payments_a[['CUSTOMER_ID', 'paymentMethodId', 'fraudulent']].drop_duplicates()
t = no_p_methods.groupby('CUSTOMER_ID').paymentMethodId.count().reset_index()
t= t.rename(columns = {'paymentMethodId':'p_methods_no'})
no_p_methods = no_p_methods.merge(t)
no_p_methods = no_p_methods.rename(columns = {'CUSTOMER_ID':'CUSTOMER_NO'})
no_p_methods = no_p_methods[['fraudulent', 'p_methods_no','CUSTOMER_NO']].drop_duplicates()

fig = go.Figure()
fig.add_trace(go.Histogram(x = no_p_methods[(no_p_methods.fraudulent)].p_methods_no,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = no_p_methods[~(no_p_methods.fraudulent)].p_methods_no,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Payment method",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

title = 'No of customer failed payment methods vs target'
no_p_methods = payments_a[['CUSTOMER_ID', 'paymentMethodId', 'fraudulent']].drop_duplicates()
t = no_p_methods.groupby('CUSTOMER_ID').paymentMethodId.count().reset_index()
t= t.rename(columns = {'paymentMethodId':'p_methods_no'})
no_p_methods = no_p_methods.merge(t)
no_p_methods = no_p_methods.rename(columns = {'CUSTOMER_ID':'CUSTOMER_NO'})
no_p_methods = no_p_methods[['fraudulent', 'p_methods_no','CUSTOMER_NO']].drop_duplicates()

fig = go.Figure()
fig.add_trace(go.Histogram(x = no_p_methods[(no_p_methods.fraudulent)].p_methods_no,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = no_p_methods[~(no_p_methods.fraudulent)].p_methods_no,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Payment method",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

title = 'Fake/foreign phones numbers vs target'
numbers = customer[['customerPhone', 'CUSTOMER_ID']]
numbers = numbers.merge(target)
numbers['customerPhone_clean'] = numbers['customerPhone'].str.replace('x.*', '', regex=True)
numbers['customerPhone_clean'] = numbers['customerPhone_clean'].str.replace('[^0-9]', '', regex = True)
numbers['num_len'] = numbers['customerPhone_clean'].str.len()

fig = go.Figure()
fig.add_trace(go.Histogram(x = numbers[(numbers.fraudulent)].num_len,
                            name = 'Fraudulent')
                        )
fig.add_trace(go.Histogram(x = numbers[~(numbers.fraudulent)].num_len,
                            name = 'nonFraudulent')
                        )
fig.update_layout(xaxis_title="Length of cleaned phone number",
                  yaxis_title="Orders count",
                  title = title)
fig.update_traces(opacity=0.75)
figures[title] = fig
#fig.show()

for f in figures:
    figures[f].show()
    
    
###EXPLORATORY ANALYSIS DONE
