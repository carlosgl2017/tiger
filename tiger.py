from datetime import date
import pandas as pd
import json
import requests 
#Globals vars
year="22"
month="05"
day="25"
namesc="\SC"+year+month+day+".CSN"
nameah="\AH"+year+month+day+".CSN"
namepf="\PF"+year+month+day+".CSN"
namecr="\CR"+year+month+day+".CSN"

#------------------------------Begin ah-------------------------
url = requests.get("http://localhost:8080/tigerah/procedureah")  
url = requests.get("http://localhost:8080/tigerah/sel")

text =url.text
data = json.loads(text)
myvar = pd.DataFrame(data)
#this is a section to trim all columns
def trim_all_columns(myvar):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return myvar.applymap(trim_strings)

#reordering the columns to dataframe
myvar = myvar[['tipent','codent','codage','codcue','codcli','fecpro','fecini','fecurt','tipcue','salcue','salret','salgar','saotga','salotr','tascue','tipmon','concue','monurt']]
myvar.columns=["TIPENT","CODENT","CODAGE","CODCUE","CODCLI","FECPRO","FECINI","FECURT","TIPCUE","SALCUE","SALRET","SALGAR","SAOTGA","SALOTR","TASCUE","TIPMON","CONCUE","MONURT"]
#convertir a tipo datatime
myvar['FECPRO'] = pd.to_datetime(myvar['FECPRO'])
myvar['FECINI'] = pd.to_datetime(myvar['FECINI'])
myvar['FECURT'] = pd.to_datetime(myvar['FECURT'])
#convertir a str y format
myvar['FECPRO'] = myvar['FECPRO'].dt.strftime('%d/%m/%Y')
myvar['FECINI'] = myvar['FECINI'].dt.strftime('%d/%m/%Y')
myvar['FECURT'] = myvar['FECURT'].dt.strftime('%d/%m/%Y')
formatted_myvar = trim_all_columns(myvar)
formatted_myvar.to_csv(r'C:\xampp\htdocs\phyton\Tiger'+nameah, index=None, sep=',', mode='a')
#------------------------------Ends ah----------------------
#print(formatted_myvar)
#print(formatted_myvar.info())

#------------------------------Begin sc-------------------------
url1 = requests.get("http://localhost:8080/tigerasc/proceduresc")  
url1 = requests.get("http://localhost:8080/tigersc/sel")

text1 =url1.text
data1 = json.loads(text1)
df1 = pd.DataFrame(data1)
#this is a section to trim all columns
def trim_all_columns(df1):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df1.applymap(trim_strings)

#reordering the columns to dataframe
df1 =   df1[['tipent','codent','codage','codcli','codide','fecpro','fecini','fecnac','fecmov','caecue','tipest','tipper','tipgen','cancah','candpf','cancre','cancan','cancae','moncan','moncae']]
df1.columns=["TIPENT","CODENT","CODAGE","CODCLI","CODIDE","FECPRO","FECINI","FECNAC","FECMOV","CAECUE","TIPEST","TIPPER","TIPGEN","CANCAH","CANDPF","CANCRE","CANCAN","CANCAE","MONCAN","MONCAE"]
#convertir a tipo datatime
df1['FECPRO'] = pd.to_datetime(df1['FECPRO'])
df1['FECINI'] = pd.to_datetime(df1['FECINI'])
df1['FECNAC'] = pd.to_datetime(df1['FECNAC'])
df1['FECMOV'] = pd.to_datetime(df1['FECMOV'])
#convertir a str y format
df1['FECPRO'] = df1['FECPRO'].dt.strftime('%d/%m/%Y')
df1['FECINI'] = df1['FECINI'].dt.strftime('%d/%m/%Y')
df1['FECNAC'] = df1['FECNAC'].dt.strftime('%d/%m/%Y')
df1['FECMOV'] = df1['FECMOV'].dt.strftime('%d/%m/%Y')
formatted_df1 = trim_all_columns(df1)
formatted_df1.to_csv(r'C:\xampp\htdocs\phyton\Tiger'+namesc, index=None, sep=',', mode='a')
#------------------------------Ends sc----------------------

#------------------------------Begin pf---------------------
url2 = requests.get("http://localhost:8080/tigerdpf/proceduredpf")  
url2 = requests.get("http://localhost:8080/tigerdpf/sel")

text2 =url2.text
data2 = json.loads(text2)
df2 = pd.DataFrame(data2)
#this is a section to trim all columns
def trim_all_columns(df2):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings2 = lambda x: x.strip() if isinstance(x, str) else x
    return df2.applymap(trim_strings2)

#reordering the columns to dataframe
df2 =   df2[['tip_ent','cod_ent','cod_age','cod_dpf','cod_soc','fec_gen','fec_ini','fec_fin','tip_cue','mon_dpf','mon_ret','pla_dpf','tas_dpf','tip_mon','con_cue','mon_dev','pag_int','opegar','fingar']]
df2.columns=["TIPENT","CODENT","CODAGE","CODCUE","CODCLI","FECPRO","FECINI","FECFIN","TIPCUE","SALCUE","SALRET","PLACUE","TASCUE","TIPMON","CONCUE","MONDEV","PAGINT","OPEGAR","FINGAR"]
#convertir a tipo datatime
df2['FECPRO'] = pd.to_datetime(df2['FECPRO'])
df2['FECINI'] = pd.to_datetime(df2['FECINI'])
df2['FECFIN'] = pd.to_datetime(df2['FECFIN'])
#convertir a str y format
df2['FECPRO'] = df2['FECPRO'].dt.strftime('%d/%m/%Y')
df2['FECINI'] = df2['FECINI'].dt.strftime('%d/%m/%Y')
df2['FECFIN'] = df2['FECFIN'].dt.strftime('%d/%m/%Y')
formatted_df2 = trim_all_columns(df2)
formatted_df2.to_csv(r'C:\xampp\htdocs\phyton\Tiger'+namepf, index=None, sep=',', mode='a')
#------------------------------Ends sc----------------------

#------------------------------Begin pf-------------------------
url3 = requests.get("http://localhost:8080/tigercr/procedurecr")  
url3 = requests.get("http://localhost:8080/tigercr/sel")

text3 =url3.text
data3 = json.loads(text3)
df3 = pd.DataFrame(data3)
#this is a section to trim all columns
def trim_all_columns(df3):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings3 = lambda x: x.strip() if isinstance(x, str) else x
    return df3.applymap(trim_strings3)

#reordering the columns to dataframe
df3 =   df3[['tip_ent','cod_ent','cod_age','cod_cre','cod_ide','fec_gen','fec_ini','fec_pri','fec_inc','fec_ucp','fec_upi','fec_ppc','fec_ppi','fec_rep','fec_cam','fec_fin','fre_pag','mon_des','mon_cuo','mon_pre','pla_cue','dia_pag','tas_cre','sal_cre','tip_est','tip_cre','tip_gar','tip_cal','tip_mon','can_mor','can_rep','can_dgr','cae_cre','ofi_cue','obj_cue','con_cue','mon_dev','tip_cuo','imp_dif']]
df3.columns=["TIPENT","CODENT","CODAGE","CODCUE","CODCLI","FECPRO","FECINI","FECPRI","FECINC","FECUPC","FECUPI","FECPPC","FECPPI","FECREP","FECCAM","FECFIN","FREPAG","MONCUE","MONCUO","MONPRE","PLACUE","DIAFIJ","TASCUE","SALCUE","TIPEST","TIPCUE","TIPGAR","TIPCAL","TIPMON","CANMOR","CANREP","CANDGR","CAECUE","OFICUE","OBJCUE","CONCUE","MONDEV","TIPCUO","IMPDIF"]
#convertir a tipo datatime
df3['FECPRO'] = pd.to_datetime(df3['FECPRO'])
df3['FECINI'] = pd.to_datetime(df3['FECINI'])
df3['FECPRI'] = pd.to_datetime(df3['FECPRI'])
df3['FECINC'] = pd.to_datetime(df3['FECINC'])
df3['FECUPC'] = pd.to_datetime(df3['FECUPC'])
df3['FECUPI'] = pd.to_datetime(df3['FECUPI'])
df3['FECPPC'] = pd.to_datetime(df3['FECPPC'])
df3['FECPPI'] = pd.to_datetime(df3['FECPPI'])
df3['FECREP'] = pd.to_datetime(df3['FECREP'])
df3['FECCAM'] = pd.to_datetime(df3['FECCAM'])
df3['FECFIN'] = pd.to_datetime(df3['FECFIN'])
#convertir a str y format
df3['FECPRO'] = df3['FECPRO'].dt.strftime('%d/%m/%Y')
df3['FECINI'] = df3['FECINI'].dt.strftime('%d/%m/%Y')
df3['FECPRI'] = df3['FECPRI'].dt.strftime('%d/%m/%Y')
df3['FECINC'] = df3['FECINC'].dt.strftime('%d/%m/%Y')
df3['FECUPC'] = df3['FECUPC'].dt.strftime('%d/%m/%Y')
df3['FECUPI'] = df3['FECUPI'].dt.strftime('%d/%m/%Y')
df3['FECPPC'] = df3['FECPPC'].dt.strftime('%d/%m/%Y')
df3['FECPPI'] = df3['FECPPI'].dt.strftime('%d/%m/%Y')
df3['FECREP'] = df3['FECREP'].dt.strftime('%d/%m/%Y')
df3['FECCAM'] = df3['FECCAM'].dt.strftime('%d/%m/%Y')
df3['FECFIN'] = df3['FECFIN'].dt.strftime('%d/%m/%Y')
formatted_df3 = trim_all_columns(df3)
formatted_df3.to_csv(r'C:\xampp\htdocs\phyton\Tiger'+namecr, index=None, sep=',', mode='a')
#------------------------------Ends sc----------------------
