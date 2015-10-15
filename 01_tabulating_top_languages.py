# -*- coding: utf-8 -*-
#歧視無邊，回頭是岸。鍵起鍵落，情真情幻。

import pyCountryGroup
import pyGeolinguisticSize
import logging

##    >>> pyCountryGroup.wp
##    <class 'pandas.core.panel.Panel'>
##    Dimensions: 4 (items) x 266 (major_axis) x 10 (minor_axis)
##    Items axis: BeltRoad to worldbank
##    Major_axis axis: ABW to _IC
##    Minor_axis axis: countrycode2 to region

##    >>> pyGeolinguisticSize.simplified
##    <class 'pandas.core.panel.Panel'>
##    Dimensions: 5 (items) x 1307 (major_axis) x 29 (minor_axis)
##    Items axis: IH to PPPGDP
##    Major_axis axis: (en, AC) to (tn, ZW)
##    Minor_axis axis: ISO to LP


in_BeltRoad = pyCountryGroup.wp['BeltRoad'].query('inBeltRoad==True')
in_BeltRoad_list_cc = list(in_BeltRoad.index.values)
in_BeltRoad_list_noCHN = in_BeltRoad_list_cc
in_BeltRoad_list_noCHN.remove("CHN")
in_BeltRoad_noCHN = in_BeltRoad.loc[in_BeltRoad_list_noCHN]

# Working dataframe df
df = pyGeolinguisticSize.simplified

def in_or_out(x):
    if x in in_BeltRoad_list_noCHN:
        return True
    if x in ["CHN","HKG","MAC"]:
        return "CHN"
    else:
        return False

df['ISO_in'] = [in_or_out(x) for x in df.ISO]

## Fundamental Categorizing/Sorting 
df=df.sort_values(['ISO_in','LP'], ascending=[False,False]).reset_index() 
#>>> df.columns
#Index(['tag', 'type', 'l_name', 'ISO', 'geo', 'geo_name', 'officialStatus',
#       'references', 'LP', 'IPop', 'PPPGDP', 'IPv4', 'ISO_in'],
#      dtype='object')
#>>> len(set(df.tag))
#>>> len(df.tag)
#1307



## Assigning global ranking numbers: geo-ling pairs
list_to_rank=['LP', 'IPop', 'PPPGDP', 'IPv4']

df['dummy']=[True]*len(df)

list_ranked = []
for col in list_to_rank:
    colname_add = col + "_ranked"
    list_ranked.append(colname_add)
#   -- Group and then rank
#    df[colname_add]=df.groupby('ISO_in')[col].rank(ascending=False) 
#   -- Global rank with dummy
    df[colname_add]=df.groupby('dummy')[col].rank(ascending=False) 

# Development needed:
#   - leverage Internet: df.query('(LP_ranked<IPop_ranked) and (IPop_ranked<PPPGDP_ranked)  ')
#   - invest Internet: df.query('(LP_ranked<IPop_ranked) and (PPPGDP_ranked<IPop_ranked)  ')
# Development achieved:
#   - leverage Internet users and encourage Internet consumption: df.query('(LP_ranked>PPPGDP_ranked) and (PPPGDP_ranked>IPop_ranked)  ')
#   - invest Internet infrastructuremore: df.query('(LP_ranked>IPop_ranked) and (IPop_ranked>PPPGDP_ranked)  ')



## Assigning global ranking numbers: ling
def aggregate2ling(dataframe_in):
    dl=dataframe_in.groupby(['l_name']).sum()
    dl['dummy']=[True]*len(dl)

    #Fundamental Categorizing/Sorting 
    dl=dl.sort_values(['LP'], ascending= False).reset_index() 

    list_ranked = []
    for col in list_to_rank:
        colname_add = col + "_ranked"
        list_ranked.append(colname_add)
        dl[colname_add]=dl.groupby('dummy')[col].rank(ascending=False) 
    return dl
# Development needed:
#   - leverage Internet: dl.query('(LP_ranked<IPop_ranked) and (IPop_ranked<PPPGDP_ranked)  ')
#   - invest Internet: dl.query('(LP_ranked<IPop_ranked) and (PPPGDP_ranked<IPop_ranked)  ')
# Development achieved:
#   - leverage Internet users and encourage Internet consumption: dl.query('(LP_ranked>PPPGDP_ranked) and (PPPGDP_ranked>IPop_ranked)  ')
#   - invest Internet infrastructure more with economic surplus: dl.query('(LP_ranked>IPop_ranked) and (IPop_ranked>PPPGDP_ranked)  ')

df_ling = aggregate2ling(df)


## Assigning geocodes under each ling: geo-ling pairs
def export_output(dataframe, filename_out):
    dataframe.to_csv(filename_out, sep="\t", float_format='%4.2f', index=True)
    filename_out = filename_out.replace("tsv","csv")
    dataframe.to_csv(filename_out, sep=",", float_format='%4.2f', index=True)

df_ = df

df_ling=df_ling.set_index(['l_name'])

df_ling['geos'] = df.groupby(['l_name'])['geo'].apply(lambda x: "[%s]" % ', '.join(x)).to_frame()
#  those that are included in the Belt and Road Initiative
df_ling['geos_OBOR'] = df[df.ISO_in == True].groupby(['l_name'])['geo'].apply(lambda x: "[%s]" % ', '.join(x)).to_frame()

export_output(df_ling, "geoling.tsv")

## Working on those that are included in the Belt and Road Initiative: ling
df_ling_OBOR = aggregate2ling(df[df.ISO_in == True])
df_ling_OBOR['geos_OBOR'] = [df_ling['geos_OBOR'][l] for l in df_ling_OBOR.l_name]

df_ling_OBOR=df_ling_OBOR.set_index(['l_name'])
export_output(df_ling_OBOR, "geoling_OBOR.tsv")

df_ = df_ling_OBOR.reset_index()

list_ranked_OBOR = []
for col in list_to_rank:
    colname_add = col + "_rOBOR"
    list_ranked_OBOR.append(colname_add)
    df_[colname_add]=df_.groupby('dummy')[col].rank(ascending=False) 



## Generating reports for top20
dict_label ={
    "l_name":"Language Name",
    "LP":"Population (LP)",
    "IPop":"Internet Population (IPop)",
    "PPPGDP":"Economy (PPPGDP)",
    "IPv4":"Internet addresses (IPv4)",
    "LP_rOBOR":"Ranking (LP)",
    "IPop_rOBOR":"Ranking (IPop)",
    "PPPGDP_rOBOR":"Ranking (PPPGDP)",
    "IPv4_rOBOR":"Ranking (IPv4)",
    "geos":"including regions ...",
    }

    
for col in list_ranked_OBOR:
    indicator=col.replace("_rOBOR","")
    top20_lang=df_.sort_values([col], ascending=True)[0:20]

    col_included_for_reports = ["l_name"] + list_to_rank + list_ranked_OBOR
    top20_lang=top20_lang[col_included_for_reports]

    # attach 'including regions ...'
    top20_lang['geos'] = [df_ling['geos_OBOR'][l]for l in list(top20_lang.l_name)]

    top20_lang[list_ranked_OBOR] = top20_lang[list_ranked_OBOR].astype(int)
    top20_lang.columns=[dict_label.get(x) for x in top20_lang.columns]
    filename_out = "top20_lang_{}.tsv".format(indicator)
    top20_lang.to_csv(filename_out, sep="\t", float_format='%4.2f', index=False)
    filename_out = "top20_lang_{}.csv".format(indicator)
    top20_lang.to_csv(filename_out, sep=",", float_format='%4.2f', index=False)



## Generating reports for the overall picture
df_overall = df.groupby('ISO_in').sum()[list_to_rank]
df_overall.columns=[dict_label.get(x) for x in df_overall.columns]
df_overall_pcts = df_overall/df_overall.sum()

df_overall_pcts.apply(lambda x: 100*x)
df_overall_pcts = df_overall_pcts.reset_index()

filename_out = "overall_world.tsv"
df_overall_pcts.to_csv(filename_out, sep="\t", float_format='%4.2f', index=False)
filename_out = filename_out.replace("tsv","csv")
df_overall_pcts.to_csv(filename_out, sep=",", float_format='%4.2f', index=False)


