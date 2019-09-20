import pandas as pd

df = pd.read_csv("Games_2015_2017.csv")

#### 1. Outcomes ####

# Regular season
df_reg = df[df['Game.ID'] < 30000]
print("Regular Season Outcomes")
print("Regular:", round(df_reg[df_reg['TOI'] == 60].shape[0] / df_reg.shape[0], 2))
print("OT:", round(df_reg[(df_reg['TOI'] > 60) & (df_reg['TOI'] < 65)].shape[0] / df_reg.shape[0], 2))
print("Shootout:", round(df_reg[df_reg['TOI'] == 65].shape[0] / df_reg.shape[0], 2))

# Playoffs
df_playoff = df[df['Game.ID'] >= 30000]
print("\nPlayoff Outcomes")
print("Regular:", round(df_playoff[df_playoff['TOI'] == 60].shape[0] / df_playoff.shape[0], 2))
print("OT:", round(df_playoff[df_playoff['TOI'] > 60].shape[0] / df_playoff.shape[0], 2))


#### 2. Goal Differential ####

# Get Goal Differential
df['GD'] = df.apply(lambda row: abs(row['GF'] - row['GA']), axis=1)

# Only non OT and Shootouts
df = df[df['TOI'] <= 60]

# Regular season
df_reg = df[df['Game.ID'] < 30000]
print("\nRegular Season Goal Differential")
reg_gd = df_reg['GD'].value_counts().to_dict()
reg_gd_sum = sum([reg_gd[key] for key in reg_gd.keys()])
print({key: round(reg_gd[key]/reg_gd_sum, 3) for key in reg_gd})

# Playoffs
df_playoff = df[df['Game.ID'] >= 30000]
print("\nPlayoff Goal Differential")
playoff_gd = df_playoff['GD'].value_counts().to_dict()
playoff_gd_sum = sum([playoff_gd[key] for key in playoff_gd.keys()])
print({key: round(playoff_gd[key]/playoff_gd_sum, 3) for key in playoff_gd})




