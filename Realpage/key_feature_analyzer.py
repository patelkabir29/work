from sklearn.ensemble import RandomForestRegressor
import pandas as pd

df = pd.read_csv(r"C:\Users\patel\Downloads\GEOGRAPHY_DATA.csv")

non_numeric_cols = df.select_dtypes(include=['object', 'datetime']).columns.tolist()
print("Non-numeric columns:", non_numeric_cols)

encode_cols = ['Category', 'Niche']

df = df[df['Agg Type'] != 'Quarterly']
df = df.drop(['Period', 'Agg Type', 'Geography Name', 'Submarket Name', 'Geography Type', 'Market Name', 'State', 'Unique Id'], axis=1)

# Encode categorical variables
df = pd.get_dummies(df, columns=encode_cols, drop_first=True)

# Fill missing values
df = df.fillna(df.median(numeric_only=True))



cols = ['Effective Rent', 'Effective RPSF', 'Occupancy Change', 'Occupancy', 'Percent of Units Offering Concessions', 'Asking Rent', 'Rent Roll (Rev / OSF)']
col_rank = {}
for col in cols:
    # Define input and target
    print(f"Analyzing column: {col}")
    X = df.drop([f'{col}'], axis=1)
    y = df[f'{col}']

    model = RandomForestRegressor()
    model.fit(X, y)

    # Get importance
    importances = pd.Series(model.feature_importances_, index=X.columns).sort_values(ascending=False)
    # Export the importances to a CSV file
    importances.to_csv(f"{col}_importances.csv", header=True)
    print(importances.head(10))
    col_rank[col] = importances
