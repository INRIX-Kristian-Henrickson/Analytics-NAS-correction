# Analytics-NAS-correction
Scripts for anomaly detection and correction on the road segment NAS

You can find a high-level overview of this project on confluence <a href=http://mobileweb2:8090/display/AN/NAS+Anomaly+Detection+and+Correction>here</a>

There are two primary spark / python scripts that do most of the work. Step 1 selects the segments that will be candidates for correction and summarizes the relavant data to make it possible to 1) assess whether each time bin should be replaced, 2) generate replacement values for the selected bins, and 3) combine the replacement values with existing NAS to complate all bins for all selected segments. Step 2 generates the replacement values, identifies the problematic segments and time bins, and combines the replacement values with existing NAS. 

The output from step is essentially complete, in that it contains all of the NAS bins for the corrected segments with both the origional NAS and replacement values. However, in addition to the information needed to address the anomalies present in NAS, it contains error flags and other values deemed useful in diagnostics. Some additional processing will be required to reduce the schema, convert attribute data types, and combine the corrected data with the un-corrected NAS data. 
