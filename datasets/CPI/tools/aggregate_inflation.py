import pandas as pd

def calculate(df_orig, metadata_df=None):
    df = df_orig.copy()
    period_ends = df['period_end'].unique()
    labels = df['label'].unique()    
    
    delete_periods = []
    
    for label in labels:
        for period in period_ends:
            if period in delete_periods:
                continue
            # Check if the label and period combination exists in the dataframe
            if not ((df['label'] == label) & (df['period_end'] == period)).any():
                # If it doesn't exist, add the period to the delete list
                delete_periods.append(period)                
    
    # Remove periods that are not present in the dataframe
    df = df[~df['period_end'].isin(delete_periods)]        

    # Calculate past year CPI - use that and the weights to calculate the inflation 
    df['CPI_Past_Year'] = df['CPI']/((100+df['CPI_YoY'])/100)
    df['basket_weights'] = df['weight']/df['weight'].sum()

    # Group by period_end and calculate aggregate index for CPI_Past_Year and CPI using weights
    # Retain other columns for later use
    agg_df = df.groupby('period_end').apply(
        lambda x: pd.Series({
            'CPI_Past_Year': (x['CPI_Past_Year'] * x['basket_weights']).sum(),
            'CPI': (x['CPI'] * x['basket_weights']).sum(),
            'weight': x['weight'].sum(),
            'period_end': x['period_end'].iloc[0],
        })).reset_index(drop=True)   
    
    agg_df['CPI_YoY'] = agg_df['CPI']/agg_df['CPI_Past_Year'] * 100 - 100

    #drop CPI_Past_Year
    agg_df.drop(columns=['CPI_Past_Year'], inplace=True)
    
    agg_df['period_end'] = pd.to_datetime(agg_df['period_end'])

    #calculate basket item weights using metadata_df
    #get all weights from metadata_df with codetype and code matching elements in df
    codes = df['code'].unique()
    constituents_df = metadata_df[metadata_df['code'].isin(codes) & (metadata_df['codetype'] == 'item_code')]
    constituents_df = constituents_df[['code', 'label', 'weight']].copy()    
    
    return agg_df, constituents_df
    
    
    
