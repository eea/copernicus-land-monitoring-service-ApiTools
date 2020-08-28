# REST API client script for batch access to Copernicus Land Monitoring Service data on WEkEO.

Publication date XXXXXXXX  
License: CC-BY (not decided yet)  
Script Version 1.0  
Contact: copernicus@eea.europa.eu  

Requirements: Python Version 2/3??  
Packages: see imports below  

This Python script allows you to do REST queries to the Copernicus Land Monitoring Service (CLMS) HTTP API.
It forsees capabilities for search and download CLMS products (currently only the Sentinel 2 based prducts of HR-S&I). Users are recommended to rely on this python script, to perform custom and automatic queries.  
Users should notice that using the interactive web interface also generates the corresponding REST query, that can be ingested directly by this script.

# Legal notice about Copernicus data:  
Access to data is based on a principle of full, open and free access as established by the Copernicus data and information policy Regulation (EU)
No 1159/2013 of 12 July 2013. This regulation establishes registration and licensing conditions for GMES/Copernicus users and can be found here:
http://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32013R1159.  

Free, full and open access to this data set is made on the conditions that:  

1. When distributing or communicating Copernicus dedicated data and Copernicus service information to the public, users shall inform the public of the source of that data and information.
2. Users shall make sure not to convey the impression to the public that the user's activities are officially endorsed by the Union.
3. Where that data or information has been adapted or modified, the user shall clearly state this.
4. The data remain the sole property of the European Union. Any information and data produced in the framework of the action shall be the sole property of the European Union. Any communication and publication by the beneficiary shall acknowledge that the data were produced "with funding by the European Union".

