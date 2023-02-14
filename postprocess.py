from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd

sparql = SPARQLWrapper("https://landregistry.data.gov.uk/landregistry/query")
sparql.setReturnFormat(JSON)

sparql.setQuery("""

prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix sr: <http://data.ordnancesurvey.co.uk/ontology/spatialrelations/>
prefix ukhpi: <http://landregistry.data.gov.uk/def/ukhpi/>
prefix lrppi: <http://landregistry.data.gov.uk/def/ppi/>
prefix skos: <http://www.w3.org/2004/02/skos/core#>
prefix lrcommon: <http://landregistry.data.gov.uk/def/common/>

# Returns house price index average prices for flats, semis, detacted and terraced for all LA since 2015, with GSS codes

SELECT
  ?regionName ?code ?date ?hpi ?hpiDetached ?hpiFlatMaisonette ?hpiSemiDetached ?hpiTerraced ?averagePriceDetached ?averagePriceFlatMaisonette ?averagePriceSemiDetached ?averagePriceTerraced
WHERE
{

  ?x
    ukhpi:refRegion ?region;
    ukhpi:refMonth ?date;
    ukhpi:housePriceIndex ?hpi;
    ukhpi:housePriceIndexFlatMaisonette ?hpiFlatMaisonette;
    ukhpi:averagePriceFlatMaisonette ?averagePriceFlatMaisonette;
    ukhpi:refPeriodStart ?startdate.
  
  OPTIONAL{
    ?x ukhpi:housePriceIndexDetached ?hpiDetached;
    ukhpi:housePriceIndexSemiDetached ?hpiSemiDetached;
    ukhpi:housePriceIndexTerraced ?hpiTerraced;
    ukhpi:averagePriceSemiDetached ?averagePriceSemiDetached;
    ukhpi:averagePriceTerraced ?averagePriceTerraced;
    ukhpi:averagePriceDetached ?averagePriceDetached.
    }

  ?region owl:sameAs ?code.
#  ?region rdfs:seeAlso ?codey #can also use this one
  ?region rdfs:label ?regionName .
# FILTER regex(str(?code), "statistics") #this also works
  FILTER contains(str(?code),"gov")
  FILTER (langMatches( lang(?regionName), "EN")&&
         ?startdate > "2015-12-31"^^xsd:date)
}
    """
)

try:
    ret = sparql.queryAndConvert()

#     for r in ret["results"]["bindings"]:
#         print(r)
except Exception as e:
    print(e)

df=pd.json_normalize(ret["results"]["bindings"])
df['code']=df['code.value'].str.split('/',expand=True)[5]

df2=df[['regionName.value', 'code','date.value','hpi.value','hpiDetached.value','hpiFlatMaisonette.value','hpiSemiDetached.value','hpiTerraced.value', 'averagePriceDetached.value','averagePriceFlatMaisonette.value','averagePriceSemiDetached.value','averagePriceTerraced.value' ]]
df2.to_csv('landreg.csv',index=False)
