import mysql.connector , os , pandas as pd
con = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="excl"
)
cursor = con.cursor()

forder_path = r"C:\Users\piugh\OneDrive\Desktop\sql+python\DATA_ANALYST\sql_excel_prj"
for file in os.listdir(forder_path):
   if file.endswith('.csv'):
        file_path = os.path.join(forder_path, file)
        df = pd.read_csv(file_path) # ✅ specify engine
        print(df.head())
        table_name = os.path.splitext(file)[0]
        cols = ",".join([f"`{col}` VARCHAR(255)" for col in df.columns])
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols})"
        cursor.execute(create_table_query)
        for _, row in df.iterrows():
            values = ",".join([f"'{str(val).replace('\'', '\\\'')}'" for val in row])
            insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({values})"
            cursor.execute(insert_query)
        con.commit()
        print(f"✅ Imported {file} into {table_name}")
        