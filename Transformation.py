import pandas as pd

def run_transformation():
    data = pd.read_csv("zipco_transaction.csv")
    
    # Data Cleaning and transaction
    # Remove duplication
    data.drop_duplicates(inplace=True)

    # Handle missing values (filled missing numeric values with the mean or medium)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)

    # Handle missing values (fill missing string/object values with 'unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'Unknown'}, inplace=True)

    data['Date'] = pd.to_datetime(data['Date'])

    # Creating Dimension Fact Table
    # Creating Product table
    products = data[['ProductName']].copy().drop_duplicates().reset_index(drop=True)
    products.index.name = 'Product_ID'
    products = products.reset_index()

    # Creating Customer table
    customers = data[['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail']].copy().drop_duplicates().reset_index(drop=True)
    customers.index.name = 'Customer_ID'
    customers = customers.reset_index()

    # Creating staff table
    staffs = data[['Staff_Name','Staff_Email']].copy().drop_duplicates().reset_index(drop=True)
    staffs.index.name = 'Staff_ID'
    staffs = staffs.reset_index()

    # Creating Transaction Table
    transactions = data.merge(customers, on=['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail'], how='left') \
                    .merge(products, on=['ProductName'], how='left') \
                    .merge(staffs, on=['Staff_Name','Staff_Email'], how='left') \
                    [['Product_ID','Customer_ID', 'Staff_ID','Date','Quantity','UnitPrice','StoreLocation','PaymentType','PromotionApplied','Weather','Temperature','StaffPerformanceRating','CustomerFeedback','DeliveryTime_min','OrderType','DayOfWeek','TotalSales']]
    transactions.index.name = 'Transaction_ID'
    transactions = transactions.reset_index()

    # Save data as csv files
    data.to_csv('clean_data.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    staffs.to_csv('staffs.csv', index=False)
    products.to_csv('products.csv', index=False)
    transactions.to_csv('transactions.csv', index=False)

    print('Data Cleaning and Transformation Completed Successfully')