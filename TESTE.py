import pandas as pd
df = pd.read_excel('EXPORT.xlsx')
tb_dinamica = df.pivot_table(index='Data do documento', columns='Documento de vendas', values='Emissor da ordem', aggfunc='sum')

print(tb_dinamica)