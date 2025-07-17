import pandas as pd
def calculate(df):
    # Calculate past year CPI - use that and the weights to calculate the inflation 
    df['CPI_Past_Year'] = df['CPI']/((100+df['CPI_YoY'])/100)
    df['basket_weights'] = df['weight']/df['weight'].sum()

    # Group by period_end and calculate aggregate index for CPI_Past_Year and CPI using weights
    agg_df = df.groupby('period_end').apply(
        lambda x: pd.Series({
            'CPI_Past_Year': (x['CPI_Past_Year'] * x['basket_weights']).sum(),
            'CPI': (x['CPI'] * x['basket_weights']).sum()
        })
    ).reset_index()

    agg_df['CPI_YoY'] = agg_df['CPI']/agg_df['CPI_Past_Year'] * 100 - 100

    #retain only period_end, CPI and CPI_YoY
    agg_df = agg_df[['period_end', 'CPI', 'CPI_YoY']]
    agg_df['period_end'] = pd.to_datetime(agg_df['period_end'])
    return agg_df
    
    
    
