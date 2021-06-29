from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import numpy as np
import locale

# define o idioma como português brasileiro e o enconding como utf-8
locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')

# define as variáveis globais
path = 'vendas-combustiveis-m3.xls'
derivados = 'derivados'
oleodiesel = 'oleodiesel'

if __name__ == '__main__':
  spark = SparkSession.builder.appName('spark_app').getOrCreate()

# define a função main, responsável por rodar todo o código
def main(name):
        extract = read_tabela(name)
        transform = transformar(extract)
        #load = write_df(transform, name)
        #return load

# cria o dataframe a partir da leitura do arquivo .xls, preenche valores nulos com 0 e retira caracteres especiais
def read_tabela(name):
        pd_df = pd.read_excel(path, name).fillna(0)
        pd_df.columns = pd_df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        cols = pd_df.select_dtypes(include=[np.object]).columns
        pd_df[cols] = pd_df[cols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
        df = spark.createDataFrame(pd_df)
        return df

# define a função que realizas as principais transformações
def transformar(df):
        inicial_df = df
        result = formatar(df)
        print("checando somas...")
        if primeiro_check(result, inicial_df) == False:
                 consertado = consertar_valores(inicial_df)
                 formatado = formatar(consertado)
                 print("checando as somas novamente...")
                 if segundo_check(formatado, consertado) == False:
                         print("As somas estão diferentes, é preciso checar.")
                 elif segundo_check(formatado, consertado) == True:
                         print("As somas estão iguais.")
                         ajustado = ajustar(formatado)
                         print("checando as somas novamente...")
                         if segundo_check(ajustado, consertado) == False:
                                 print("As somas estão diferentes, é preciso checar.")
                         elif segundo_check(ajustado, consertado) == True:
                                 print("As somas estão iguais.")
                                 final = ajustado.drop('year')
                                 final.printSchema()
                                 final.show()
                                 return final
        elif primeiro_check(result, inicial_df) == True:
                 ajustado = ajustar(result)
                 if segundo_check(ajustado,inicial_df) == False:
                         print("As somas estão diferentes, é preciso checar.")
                 elif segundo_check(ajustado,inicial_df) == True:
                         print("As somas estão iguais.") 
                         final = ajustado.drop('year')
                         final.printSchema()
                         final.show() 
                         return final

# formata o dataframe e renomeia as colunas
def formatar(df):
        format_pd_df = df.drop('REGIAO', 'TOTAL').toDF(*['product', 'year','uf','unit','Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])
        unpivotExpr = "stack(12, 'Jan', Jan, 'Fev', Fev, 'Mar', Mar, 'Abr', Abr, 'Mai', Mai, 'Jun', Jun, 'Jul', Jul, 'Ago', Ago, 'Set', Set, 'Out', Out, 'Nov', Nov, 'Dez', Dez) as (month,volume)"
        unpivot_df = format_pd_df.select('product', 'year','uf','unit',expr(unpivotExpr))
        return unpivot_df

# primeiro check das somas
def primeiro_check(df, inicial_df):
        formatada_df = df.select('year','uf','product','unit','volume').groupby('year','uf','product','unit').agg(sum('volume').alias('volume'))
        primeira_df = inicial_df.select('ANO','ESTADO','COMBUSTIVEL','UNIDADE','TOTAL').toDF(*['year', 'uf','product','unit','TOTAL'])
        check_sum = formatada_df.join(primeira_df,how='inner', on=['year','uf','product','unit'])
        check_sum = check_sum.withColumn('flag', when(check_sum.volume == check_sum.TOTAL,0).when(check_sum.volume != check_sum.TOTAL,1))
        flag = check_sum.select('flag').filter(check_sum.flag == 1)
        if flag.count() != 0:
                print("As somas estão diferentes, é preciso checar.")
                provar_erro(check_sum,inicial_df)
                return False
        elif flag.count() == 0:
                print("As somas estão iguais")
                return True

# prova que a tabela possui um erro de soma nos dados iniciais
def provar_erro(check_sum, inicial_df):
        print("checando somas na base de dados iniciais...")
        check_sum = check_sum.withColumn('volume_igual_total', when(check_sum.volume == check_sum.TOTAL,'igual').when(check_sum.volume != check_sum.TOTAL,'diferente'))
        check_sum = check_sum.select('*').filter(check_sum.volume_igual_total == 'diferente')
        check_sum.show()
        check_total_inicial = inicial_df.select('*').filter((inicial_df.COMBUSTIVEL == 'QUEROSENE ILUMINANTE (m3)') & (inicial_df.ANO == 2020))
        check_total_inicial = check_total_inicial.withColumn('sum_meses', check_total_inicial.Jan +check_total_inicial.Fev + check_total_inicial.Mar + check_total_inicial.Abr + check_total_inicial.Mai + check_total_inicial.Jun + check_total_inicial.Jul + check_total_inicial.Ago + check_total_inicial.Set + check_total_inicial.Out + check_total_inicial.Nov + check_total_inicial.Dez)
        check_total_inicial = check_total_inicial.withColumn('sum_meses_igual_TOTAL', when(check_total_inicial.sum_meses == check_total_inicial.TOTAL,'igual').when(check_total_inicial.sum_meses != check_total_inicial.TOTAL,'diferente'))
        check_total_inicial.show()
        print("isso mostra que inicialmente os dados totais estavam errados") 

# conserta valores possivelmente errados
def consertar_valores(inicial_df):
        print("consertando valores...")
        inicial_df = inicial_df.withColumn('sum_meses', inicial_df.Jan +inicial_df.Fev + inicial_df.Mar + inicial_df.Abr + inicial_df.Mai + inicial_df.Jun + inicial_df.Jul + inicial_df.Ago + inicial_df.Set + inicial_df.Out + inicial_df.Nov + inicial_df.Dez)
        inicial_df = inicial_df.withColumn('sum_meses_igual_TOTAL', when(inicial_df.sum_meses == inicial_df.TOTAL,'igual').when(inicial_df.sum_meses != inicial_df.TOTAL,'diferente'))
        inicial_df = inicial_df.withColumn('TOTAL', when(inicial_df.sum_meses_igual_TOTAL == 'diferente', inicial_df.sum_meses).otherwise(inicial_df.TOTAL))
        inicial_df = inicial_df.drop('sum_meses', 'sum_meses_igual_TOTAL')
        print("consertado!")
        return inicial_df

# segundo check das somas
def segundo_check(df,inicial_df):
        formatada_df = df.select('year','uf','product','unit','volume').groupby('year','uf','product','unit').agg(sum('volume').alias('volume'))
        primeira_df = inicial_df.select('ANO','ESTADO','COMBUSTIVEL','UNIDADE','TOTAL').toDF(*['year', 'uf','product','unit','TOTAL'])
        check_sum = formatada_df.join(primeira_df,how='inner', on=['year','uf','product','unit'])
        check_sum = check_sum.withColumn('flag', when(check_sum.volume == check_sum.TOTAL,0).when(check_sum.volume != check_sum.TOTAL,1))
        flag = check_sum.select('flag').filter(check_sum.flag == 1)
        if flag.count() != 0:       
                return False
        elif flag.count() == 0:
                return True

# adiciona coluna de year_month, transformando mês de nome para número e concatenando com ano, reordena as colunas, passa a schema correta e cria timestamp
def ajustar(df):
        df = df.withColumn("month",from_unixtime(unix_timestamp(col("month"),'MMM'),'MM'))
        df = df.select(to_date(concat_ws('-',df.year,df.month),'yyyy-MM').alias('year_month'),'uf','product','unit','volume','year').withColumn('created_at',current_timestamp())
        return df

# salva dataframe em um arquivo .xlsx
def write_df(df, name):
        df.write.mode('overwrite').parquet('resultado/' + name)

main (derivados)
main (oleodiesel)