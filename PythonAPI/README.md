# clmsapi

Python client for accessing the Copernicus Land Monitoring Service HTTP API on WEkEO by means of REST calls. Currenty supported (on server side) are only Data from the [High Resolution Snow and Ice Monitoring](https://land.copernicus.eu/pan-european/biophysical-parameters/high-resolution-snow-and-ice-monitoring).

## Registration:
Before downloading any data you need to register [here](https://cryo.land.copernicus.eu/finder).  

## Other Data access possibilities:
Web frontend for seach and download: https://cryo.land.copernicus.eu/finder  
WMS webfrontend data viewer: https://cryo.land.copernicus.eu/browser  
WMS services:  
•	https://cryo.land.copernicus.eu/wms/FSC/  
•	https://cryo.land.copernicus.eu/wms/RLIE/  
•	https://cryo.land.copernicus.eu/wms/PSA/  

## API Url: 
https://cryo.land.copernicus.eu/resto/api/collections/HRSI/search.json
## API reference: 
https://cryo.land.copernicus.eu/resto/api/collections/HRSI/describe.xml

## Installation:
Download the script and run (use correct path):
```S
>python CLMS_downloader.py --help
```

## Legal notice about Copernicus Data:
Access to data is based on a principle of full, open and free access as established by the Copernicus data and information policy Regulation (EU) No 1159/2013 of 12 July 2013. This regulation establishes registration and licensing conditions for GMES/Copernicus users and can be found here: http://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32013R1159.  

Free, full and open access to this data set is made on the conditions that:  
1. When distributing or communicating Copernicus dedicated data and Copernicus service information to the public, users shall inform the public of the source of that data and information.  
2. Users shall make sure not to convey the impression to the public that the user's activities are officially endorsed by the Union.  
3. Where that data or information has been adapted or modified, the user shall clearly state this.  
4. The data remain the sole property of the European Union. Any information and data produced in the framework of the action shall be the sole property of the European Union. Any communication and publication by the beneficiary shall acknowledge that the data were produced “with funding by the European Union”.  
